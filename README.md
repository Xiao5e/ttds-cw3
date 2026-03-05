# Personal Research Digest Search Engine (TTDS CW3)

The rapid growth of Web text content makes efficient and scalable information retrieval systems increasingly important. For many users, the main challenges in search are finding the exact information they need, discovering related information, and tracking content that is constantly updated.

In this project, we aim to build a lightweight and practical retrieval system for a personal research digest that can be used daily. The system continuously collects articles from multiple RSS-based information sources and organises them into a searchable corpus, enabling users to quickly review past materials and discover newly published content with low latency. In this setting, users may perform exploratory search to browse a topic, or submit more precise queries to locate a specific statement or event. Therefore, the system is designed to support flexible query formulations while maintaining fast response at the scale of over one hundred thousand documents.

Overall, this project demonstrates an end-to-end retrieval system for a realistic and dynamically changing web content scenario, with a focus on ease of use, timeliness, and reliable online access. The system not only implements core information retrieval functionality, but also adopts a clear architectural design for scalability, maintainability, and practical deployment.

## Key Features
- BM25-based ranking with optional Pseudo-Relevance Feedback (PRF)
- Boolean queries, phrase search, and proximity search (positional inverted index)
- Time-range filtering and optional freshness-boosted sorting
- Scheduler-driven RSS ingestion and incremental indexing
- Web UI for interactive search with paging and snippets
- Online deployment support (FastAPI backend + reverse proxy)

## Repository Structure
- `backend/`: FastAPI service (indexing, ingestion, retrieval APIs)
- `frontend/`: Vue-based web UI

## Quick Start (Local)
Start backend first (port 8000):
```bash
cd backend
pip install -r requirements.txt
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

Start frontend (port 5173):
cd frontend
npm install
npm run dev
Open: http://127.0.0.1:5173
Note: Vite dev server proxies /search, /health, and /admin/* to the backend.

## Main API Endpoints
POST /search: search over the indexed corpus
POST /admin/ingest: fetch new documents and update the index
GET /health: lightweight health checks
