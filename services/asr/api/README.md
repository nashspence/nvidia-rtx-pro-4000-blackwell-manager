# Parakeet REST sidecar

Routes:

- `GET /health/live`
- `GET /health/ready`
- `GET /v1/models`
- `POST /v1/audio/transcriptions`

Example:

```bash
curl -s http://localhost:8000/v1/audio/transcriptions \
  -F file=@sample.opus \
  -F language=multi \
  -F response_format=verbose_json \
  -F 'timestamp_granularities[]=word' \
  -F 'timestamp_granularities[]=segment'
```
