# TTDS CW3 Demo Frontend (Vue)

## Run (local)

1) Start backend first (port 8000):
```bash
cd ../backend
pip install -r requirements.txt
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

2) Start frontend (port 5173):
```bash
cd ../frontend
npm install
npm run dev
```

Open: http://127.0.0.1:5173

Note: Vite dev server proxies /search, /health, /admin/* to the backend.
