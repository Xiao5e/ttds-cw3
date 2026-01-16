from __future__ import annotations
from typing import Optional, List, Dict, Literal
from pydantic import BaseModel, Field

class Document(BaseModel):
    doc_id: str
    title: str
    body: str
    url: Optional[str] = None
    timestamp: Optional[str] = None  # ISO string
    lang: str = "en"

class SearchFilters(BaseModel):
    lang: Optional[str] = None
    time_from: Optional[str] = None  # ISO
    time_to: Optional[str] = None    # ISO
    field: Optional[Literal["title", "body"]] = None

class SearchRequest(BaseModel):
    query: str = Field(..., min_length=1)
    top_k: int = 10
    use_prf: bool = False
    filters: Optional[SearchFilters] = None

class SearchResult(BaseModel):
    doc_id: str
    title: str
    snippet: str
    score: float
    url: Optional[str] = None
    timestamp: Optional[str] = None
    lang: str = "en"

class SearchResponse(BaseModel):
    query: str
    took_ms: int
    total_hits: int
    results: List[SearchResult]

class IngestRequest(BaseModel):
    docs: List[Document]

class IngestResponse(BaseModel):
    ingested: int
    updated_index: bool
    index_version: str

class HealthResponse(BaseModel):
    status: str
    index_version: str
    docs_count: int
