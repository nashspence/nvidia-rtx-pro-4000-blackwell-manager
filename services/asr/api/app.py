import json
import logging
import os
import shutil
import subprocess
import tempfile
import time
from contextlib import asynccontextmanager
from pathlib import Path

import grpc
import httpx
import riva.client
from fastapi import FastAPI, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse

RIVA_GRPC_URI = os.getenv("RIVA_GRPC_URI", "parakeet-tdt:50051")
NIM_READY_URL = os.getenv("NIM_READY_URL", "http://parakeet-tdt:9000/v1/health/ready")
MAX_UPLOAD_MB = int(os.getenv("MAX_UPLOAD_MB", "512"))
GRPC_MAX_MB = int(os.getenv("GRPC_MAX_MB", str(MAX_UPLOAD_MB)))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

LANGUAGE_CODE = "multi"
SEGMENT_GAP_S = 0.8

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("parakeet-rest")
state = {}


def sh(*args: str) -> str:
    p = subprocess.run(args, capture_output=True, text=True)
    if p.returncode:
        raise RuntimeError((p.stderr or p.stdout or "command failed").strip())
    return p.stdout.strip()


def seconds(value) -> float:
    if isinstance(value, (int, float)):
        return round(float(value) / 1000.0, 3)
    return round(float(getattr(value, "seconds", 0)) + float(getattr(value, "nanos", 0)) / 1e9, 3)


def build_segments(words: list[dict]) -> list[dict]:
    if not words:
        return []

    segments, current = [], []

    def flush() -> None:
        nonlocal current
        if not current:
            return
        text = " ".join(w["word"] for w in current).strip()
        segments.append(
            {
                "id": len(segments),
                "start": current[0]["start"],
                "end": current[-1]["end"],
                "text": text,
                "words": current.copy(),
            }
        )
        current = []

    for word in words:
        if current:
            gap = word["start"] - current[-1]["end"]
            punct_break = current[-1]["word"].endswith((".", "!", "?"))
            if gap >= SEGMENT_GAP_S or punct_break:
                flush()
        current.append(word)

    flush()
    return segments


def preprocess_to_flac(src: Path, dst: Path) -> None:
    sh(
        "ffmpeg",
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-i",
        str(src),
        "-vn",
        "-map_metadata",
        "-1",
        "-ac",
        "1",
        "-ar",
        "16000",
        "-sample_fmt",
        "s16",
        "-c:a",
        "flac",
        str(dst),
    )


def recognize(flac_bytes: bytes):
    config = riva.client.RecognitionConfig(
        language_code=LANGUAGE_CODE,
        max_alternatives=1,
        enable_automatic_punctuation=True,
        enable_word_time_offsets=True,
        verbatim_transcripts=False,
    )
    return state["asr"].offline_recognize(flac_bytes, config)


def response_to_payload(resp) -> dict:
    words = []

    for result in resp.results:
        if not result.alternatives:
            continue
        alt = result.alternatives[0]
        for item in alt.words:
            words.append(
                {
                    "word": item.word,
                    "start": seconds(item.start_time),
                    "end": seconds(item.end_time),
                }
            )

    return {"segments": build_segments(words)}


async def upstream_ready() -> tuple[bool, str, list[str]]:
    try:
        r = await state["http"].get(NIM_READY_URL)
        if r.status_code != 200:
            return False, f"nim_http_{r.status_code}", []
    except Exception as e:
        return False, f"nim_http_error:{e}", []

    try:
        req = riva.client.proto.riva_asr_pb2.RivaSpeechRecognitionConfigRequest()
        cfg = await run_in_threadpool(state["asr"].stub.GetRivaSpeechRecognitionConfig, req)
        models = sorted(m.model_name for m in cfg.model_config if m.parameters.get("type") == "offline")
        return bool(models), "ready" if models else "no_offline_model", models
    except grpc.RpcError as e:
        return False, f"grpc_error:{e.details()}", []
    except Exception as e:
        return False, f"grpc_error:{e}", []


@asynccontextmanager
async def lifespan(app: FastAPI):
    state["http"] = httpx.AsyncClient(timeout=2.0)
    options = [
        ("grpc.max_receive_message_length", GRPC_MAX_MB * 1024 * 1024),
        ("grpc.max_send_message_length", GRPC_MAX_MB * 1024 * 1024),
    ]
    auth = riva.client.Auth(uri=RIVA_GRPC_URI, use_ssl=False, options=options)
    state["asr"] = riva.client.ASRService(auth)
    ok, detail, models = await upstream_ready()
    log.info("startup ready=%s detail=%s models=%s", ok, detail, models)
    yield
    await state["http"].aclose()


app = FastAPI(title="Parakeet REST", version="1.0.0", lifespan=lifespan)


@app.get("/health/live")
async def health_live():
    return {"status": "ok"}


@app.get("/health/ready")
async def health_ready():
    ok, detail, models = await upstream_ready()
    body = {"status": "ready" if ok else "not_ready", "detail": detail, "models": models}
    return JSONResponse(status_code=200 if ok else 503, content=body)


@app.get("/v1/models")
async def models():
    ok, detail, names = await upstream_ready()
    return JSONResponse(
        status_code=200 if ok else 503,
        content={"object": "list", "data": [{"id": n, "object": "model"} for n in names], "detail": detail},
    )


@app.post("/v1/audio/transcriptions")
async def transcriptions(request: Request):
    form = await request.form()
    file = form.get("file")
    if file is None:
        raise HTTPException(422, "missing multipart field: file")

    t0 = time.perf_counter()

    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)
        src = tmpdir / (Path(getattr(file, "filename", "upload")).name or "upload")
        flac = tmpdir / "audio.flac"

        with src.open("wb") as f:
            shutil.copyfileobj(file.file, f)

        size_mb = src.stat().st_size / 1024 / 1024
        if size_mb > MAX_UPLOAD_MB:
            raise HTTPException(413, f"upload too large: {size_mb:.1f} MB > {MAX_UPLOAD_MB} MB")

        try:
            preprocess_to_flac(src, flac)
        except Exception as e:
            raise HTTPException(400, f"ffmpeg failed to decode input: {e}") from e

        audio = flac.read_bytes()

        try:
            resp = await run_in_threadpool(recognize, audio)
        except grpc.RpcError as e:
            raise HTTPException(502, e.details()) from e

    payload = response_to_payload(resp)

    log.info(
        json.dumps(
            {
                "event": "transcription",
                "filename": getattr(file, "filename", None),
                "language": LANGUAGE_CODE,
                "segments": len(payload["segments"]),
                "processing_seconds": round(time.perf_counter() - t0, 3),
            }
        )
    )

    return JSONResponse(content=payload)