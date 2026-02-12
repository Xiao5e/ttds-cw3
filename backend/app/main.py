from __future__ import annotations
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .schemas import SearchRequest, SearchResponse, IngestRequest, IngestResponse, HealthResponse, Document
from .storage.document_store import DocumentStore
from .storage.index_store import IndexStore
from .indexing.indexer import build_index, update_index
from .search.searcher import search as run_search
from .features.prf import expand_query
from .utils.logging import get_logger
from .ingest.scheduler import start_scheduler_task


logger = get_logger(__name__)

app = FastAPI(title="TTDS CW3 Demo Search Engine", version="0.1.0")

# Allow local dev with Vue (localhost:5173 etc.)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

STORE = DocumentStore()
INDEX = IndexStore()
SCHEDULER_TASK = None

def _load_or_seed():
    STORE.load_if_exists()
    INDEX.load_if_exists()
    if len(STORE) == 0:
        # Seed with a few hardcoded docs for demo
        seed = [
            Document(
                doc_id="doc-1",
                title="FastAPI tutorial for building search APIs",
                body="FastAPI is a modern, fast web framework for building APIs with Python. This document talks about REST, async, and deployment.",
                url="https://example.com/fastapi-search",
                timestamp="2025-12-01T12:00:00Z",
                lang="en",
            ),
            Document(
                doc_id="doc-2",
                title="BM25 ranking explained",
                body="BM25 is a strong baseline retrieval model. It uses term frequency, inverse document frequency, and document length normalization.",
                url="https://example.com/bm25",
                timestamp="2025-11-15T12:00:00Z",
                lang="en",
            ),
            Document(
                doc_id="doc-3",
                title="Incremental indexing for live search systems",
                body="Live indexing enables continuous collection of data streaming and indexing. New documents can be added without rebuilding the entire index.",
                url="https://example.com/live-indexing",
                timestamp="2025-12-20T09:00:00Z",
                lang="en",
            ),
            Document(
                doc_id="doc-4",
                title="Vue frontend for search engines",
                body="Vue can build a nice interface with a query box, snippets, pagination, and query suggestion. It is often deployed as static assets.",
                url="https://example.com/vue-ui",
                timestamp="2025-12-10T18:30:00Z",
                lang="en",
            ),
        ]
        STORE.add_documents(seed, persist=True)
        build_index(STORE.all(), INDEX)
        logger.info("Seeded demo documents and built index.")

_load_or_seed()


# Open scheduler
@app.on_event("startup")
async def _startup():
    global SCHEDULER_TASK
    print("[startup] starting scheduler...", flush=True)
    SCHEDULER_TASK = start_scheduler_task(STORE, INDEX)

# shut down scheduler
@app.on_event("shutdown")
async def _shutdown():
    global SCHEDULER_TASK
    print("[shutdown] stopping scheduler...", flush=True)
    if SCHEDULER_TASK:
        SCHEDULER_TASK.cancel()

@app.get("/health", response_model=HealthResponse)
def health():
    return HealthResponse(status="ok", index_version=INDEX.index_version, docs_count=len(STORE))

@app.post("/search", response_model=SearchResponse)
def search(req: SearchRequest):
    # PRF hook is optional; keep it simple for demo
    return run_search(req, STORE, INDEX, prf_expand=expand_query)

@app.post("/admin/ingest", response_model=IngestResponse)
def ingest(req: IngestRequest):
    ingested = STORE.add_documents(req.docs, persist=True)
    if ingested > 0:
        # update index only for the newly added docs
        # (we filter again inside update_index anyway)
        index_version = update_index(req.docs, INDEX)
        updated = True
    else:
        index_version = INDEX.index_version
        updated = False
    return IngestResponse(ingested=ingested, updated_index=updated, index_version=index_version)
