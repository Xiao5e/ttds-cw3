# TTDS CW3 Demo Backend (FastAPI)

This is a minimal runnable skeleton for the TTDS CW3 group project:
- `/search` supports BM25 ranking (demo-scale, in-memory)
- `/admin/ingest` adds new docs and updates the index incrementally
- `/health` provides basic status

## Run (local)

```bash
cd backend
python -m venv .venv
source .venv/bin/activate   # (Windows: .venv\Scripts\activate)
pip install -r requirements.txt
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Open:
- http://127.0.0.1:8000/docs  (Swagger UI)
- POST http://127.0.0.1:8000/search

## Example search payload

```json
{
  "query": "live indexing bm25",
  "top_k": 5,
  "use_prf": false,
  "filters": {"lang": "en"}
}
```
