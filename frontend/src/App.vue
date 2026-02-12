<template>
  <div class="wrap">
    <h1>TTDS CW3 Demo Search</h1>

    <form class="search" @submit.prevent="doSearch">
      <input
        v-model="query"
        class="q"
        placeholder='Try: live indexing bm25   or   "bm25 ranking"'
        autocomplete="off"
      />
      <button class="btn" :disabled="loading || !query.trim()">Search</button>
    </form>

    <div class="row">
      <label class="chk">
        <input type="checkbox" v-model="usePrf" />
        Use PRF (query expansion)
      </label>

      <label class="sel">
        Lang:
        <select v-model="lang">
          <option value="">Any</option>
          <option value="en">en</option>
        </select>
      </label>

      <label class="sel">
        Page size:
        <select v-model.number="pageSize">
          <option :value="5">5</option>
          <option :value="10">10</option>
          <option :value="20">20</option>
        </select>
      </label>

      <button class="btn2" @click="checkHealth" :disabled="loadingHealth">Health</button>
      <span class="muted" v-if="health">{{ health }}</span>
    </div>

    <div v-if="error" class="error">{{ error }}</div>

    <div v-if="meta" class="meta">
      <span><b>Total hits:</b> {{ meta.total_hits }}</span>
      <span><b>Took:</b> {{ meta.took_ms }} ms</span>
      <span><b>Showing:</b> {{ results.length }}</span>
      <span v-if="paginationReady" class="muted">page {{ currentPage }}</span>
    </div>

    <div v-if="loading" class="muted">Searching...</div>
    <div v-else-if="results.length === 0 && meta" class="muted">No results.</div>

    <ol class="results" v-if="results.length">
      <li v-for="r in results" :key="r.doc_id" class="item">
        <div class="title">
          <a v-if="r.url" :href="r.url" target="_blank" rel="noreferrer">{{ r.title }}</a>
          <span v-else>{{ r.title }}</span>
        </div>
        <div class="snippet">{{ r.snippet }}</div>
        <div class="info">
          <span class="tag">score: {{ formatScore(r.score) }}</span>
          <span class="tag">id: {{ r.doc_id }}</span>
          <span class="tag" v-if="r.timestamp">time: {{ r.timestamp }}</span>
          <span class="tag">lang: {{ r.lang }}</span>
        </div>
      </li>
    </ol>

    <!-- Pagination (max 5 pages, current page as centered as possible) -->
    <div v-if="paginationReady" class="pager">
      <button class="btn2" :disabled="currentPage <= 1" @click="goToPage(currentPage - 1)">Prev</button>

      <button
        v-for="p in pageNumbers"
        :key="p"
        class="pbtn"
        :class="{ active: p === currentPage }"
        @click="goToPage(p)"
      >
        {{ p }}
      </button>

      <button class="btn2" :disabled="!canGoNext" @click="goToPage(currentPage + 1)">Next</button>

      <span class="muted" v-if="prefetching">prefetching…</span>
    </div>
  </div>
</template>

<script>
// API contract change:
// - top_k -> size
// - add cursor: lastscore, lastid
// First page: lastscore=null, lastid=null
export default {
  data() {
    return {
      query: 'live indexing bm25',
      usePrf: false,
      lang: 'en',
      pageSize: 10,

      loading: false,
      loadingHealth: false,
      prefetching: false,
      error: '',

      results: [],
      meta: null,
      health: '',

      // pagination state
      currentPage: 1,
      windowStart: 1,
      windowEnd: 1,

      // cache
      pageCache: {}, // { [pageNum]: results[] }
      pageEndCursor: {}, // { [pageNum]: { lastscore, lastid } }
      noMoreAfterPage: null // number | null (known last page)
    }
  },
  computed: {
    paginationReady() {
      return !!this.meta && (this.windowEnd - this.windowStart + 1) >= 1
    },
    pageNumbers() {
      const nums = []
      const end = this.windowEnd
      for (let p = this.windowStart; p <= end; p++) nums.push(p)
      return nums
    },
    canGoNext() {
      if (this.noMoreAfterPage != null) return this.currentPage < this.noMoreAfterPage
      // if we don't know the end yet, allow next when next page is already cached or our window looks "full"
      return !!this.pageCache[this.currentPage + 1] || (this.windowEnd - this.windowStart + 1) === 5
    }
  },
  watch: {
    // if user changes page size, restart search with the new size
    pageSize() {
      if (this.query.trim()) this.doSearch()
    }
  },
  methods: {
    formatScore(s) {
      const n = Number(s)
      return Number.isFinite(n) ? n.toFixed(3) : String(s)
    },

    buildPayload({ size, cursor }) {
      return {
        query: this.query,
        size,
        top_k: size, // for backward compatibility, backend should ignore if both provided //----------------------------------------------------------------------------------------------------------------------
        use_prf: this.usePrf,
        last_min_bm25_score: cursor?.lastscore ?? null,
        last_max_rerank_id: cursor?.lastid ?? null,
        filters: this.lang
          ? {
              lang: this.lang,
              time_from: null,
              time_to: null,
              field: null
            }
          : null
      }
    },

    getEndCursorFromResults(items) {
      if (!items || items.length === 0) return null
      const last = items[items.length - 1]
      // backend should treat (lastscore,lastid) as the cursor
      return { lastscore: last.score, lastid: last.doc_id }
    },

    async callSearchApi({ size, cursor }) {
      const payload = this.buildPayload({ size, cursor })
      const res = await fetch('/search', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      })
      if (!res.ok) {
        const txt = await res.text()
        throw new Error(`Search failed (${res.status}): ${txt}`)
      }
      return await res.json()
    },

    resetPaginationState() {
      this.currentPage = 1
      this.windowStart = 1
      this.windowEnd = 1
      this.pageCache = {}
      this.pageEndCursor = {}
      this.noMoreAfterPage = null
    },

    // 1) fast first page
    // 2) background prefetch a 5-page window (pageSize*5)
    async doSearch() {
      const q = this.query.trim()
      if (!q) return

      this.error = ''
      this.loading = true
      this.prefetching = false
      this.results = []
      this.meta = null
      this.resetPaginationState()

      try {
        // first page: size=pageSize
        const data = await this.callSearchApi({ size: this.pageSize, cursor: null })
        const page1 = data.results || []

        this.pageCache[1] = page1
        this.pageEndCursor[1] = this.getEndCursorFromResults(page1)

        this.results = page1
        this.meta = { total_hits: data.total_hits, took_ms: data.took_ms }

        // background: prefetch 1..5
        // (We purposely do not block first render)
        this.prefetchWindow(this.currentPage)
      } catch (e) {
        this.error = e?.message || String(e)
      } finally {
        this.loading = false
      }
    },

    // Ensure we have cursor for (pageNum). Cursor we need for fetching a window starting at startPage
    // is end-cursor of (startPage-1). If missing, we backfill by fetching from the start.
    async ensureCursorForPageEnd(pageNum) {
      if (pageNum <= 0) return null
      if (this.pageEndCursor[pageNum]) return this.pageEndCursor[pageNum]

      // Fallback: fetch (pageNum*pageSize) from start, then compute cursor
      const data = await this.callSearchApi({ size: pageNum * this.pageSize, cursor: null })
      const items = data.results || []
      // fill cache for pages that we can derive
      const pages = Math.ceil(items.length / this.pageSize)
      for (let i = 0; i < pages; i++) {
        const p = i + 1
        const slice = items.slice(i * this.pageSize, (i + 1) * this.pageSize)
        if (slice.length === 0) break
        if (!this.pageCache[p]) this.pageCache[p] = slice
        if (!this.pageEndCursor[p]) this.pageEndCursor[p] = this.getEndCursorFromResults(slice)
      }
      return this.pageEndCursor[pageNum] || null
    },

    // Fetch a 5-page window centered on centerPage (or starting at 1)
    async prefetchWindow(centerPage) {
      // decide window start (current page as centered as possible)
      const start = Math.max(1, centerPage - 2)
      const cursor = start === 1 ? null : await this.ensureCursorForPageEnd(start - 1)

      this.prefetching = true
      try {
        const data = await this.callSearchApi({ size: this.pageSize * 5, cursor })
        const items = data.results || []

        // slice into pages and update cache/cursors
        const pagesInBlock = Math.ceil(items.length / this.pageSize)
        const maxPagesToShow = Math.min(5, pagesInBlock || 1)

        for (let i = 0; i < maxPagesToShow; i++) {
          const p = start + i
          const slice = items.slice(i * this.pageSize, (i + 1) * this.pageSize)
          if (slice.length === 0) break
          this.pageCache[p] = slice
          this.pageEndCursor[p] = this.getEndCursorFromResults(slice)
        }

        this.windowStart = start
        this.windowEnd = start + maxPagesToShow - 1

        // If we got less than 5 pages OR last page has < pageSize items,
        // we can treat windowEnd as the last page (no more data).
        const lastPageSlice = this.pageCache[this.windowEnd] || []
        if (maxPagesToShow < 5 || lastPageSlice.length < this.pageSize) {
          this.noMoreAfterPage = this.windowEnd
        }
      } catch (e) {
        // prefetch failure shouldn't break the already rendered page
        // but we still surface a hint
        this.error = this.error || (e?.message || String(e))
      } finally {
        this.prefetching = false
      }
    },

    async goToPage(p) {
      if (p < 1) return
      if (this.noMoreAfterPage != null && p > this.noMoreAfterPage) return

      // show cached page immediately
      const cached = this.pageCache[p]
      if (cached) {
        this.currentPage = p
        this.results = cached
      } else {
        // This should be rare because we only render page buttons for cached pages,
        // but we keep a fallback.
        this.loading = true
        try {
          const cursor = p === 1 ? null : await this.ensureCursorForPageEnd(p - 1)
          const data = await this.callSearchApi({ size: this.pageSize, cursor })
          const items = data.results || []
          this.pageCache[p] = items
          this.pageEndCursor[p] = this.getEndCursorFromResults(items)
          this.currentPage = p
          this.results = items
        } catch (e) {
          this.error = e?.message || String(e)
        } finally {
          this.loading = false
        }
      }

      // background: re-center 5-page window around current page
      this.prefetchWindow(this.currentPage)
    },

    async checkHealth() {
      this.health = ''
      this.loadingHealth = true
      try {
        const res = await fetch('/health')
        const data = await res.json()
        this.health = `status=${data.status}, docs=${data.docs_count}, index=${data.index_version}`
      } catch (e) {
        this.health = 'health check failed'
      } finally {
        this.loadingHealth = false
      }
    }
  },
  mounted() {
    this.checkHealth()
    this.doSearch()
  }
}
</script>

<style>
.wrap { max-width: 900px; margin: 32px auto; padding: 0 16px; font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; }
h1 { font-size: 22px; margin: 0 0 16px; }
.search { display: flex; gap: 10px; margin-bottom: 12px; }
.q { flex: 1; padding: 10px 12px; font-size: 14px; border: 1px solid #ccc; border-radius: 8px; }
.btn { padding: 10px 14px; border: 1px solid #333; border-radius: 8px; background: #fff; cursor: pointer; }
.btn:disabled { opacity: 0.5; cursor: not-allowed; }
.row { display: flex; align-items: center; gap: 14px; margin: 10px 0 14px; flex-wrap: wrap; }
.chk, .sel { font-size: 13px; }
.btn2 { padding: 6px 10px; border: 1px solid #666; border-radius: 8px; background: #fff; cursor: pointer; }
.meta { display: flex; gap: 18px; font-size: 13px; margin: 10px 0; flex-wrap: wrap; }
.results { padding-left: 18px; }
.item { margin: 14px 0; }
.title { font-weight: 600; margin-bottom: 6px; }
.title a { color: inherit; text-decoration: underline; }
.snippet { color: #333; font-size: 13px; line-height: 1.35; }
.info { margin-top: 6px; display: flex; gap: 8px; flex-wrap: wrap; }
.tag { font-size: 12px; border: 1px solid #ddd; padding: 2px 6px; border-radius: 999px; color: #444; }
.muted { color: #666; font-size: 13px; }
.error { background: #fff3f3; border: 1px solid #ffd0d0; padding: 10px 12px; border-radius: 8px; color: #a10000; margin: 10px 0; }

.pager { margin: 16px 0 28px; display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
.pbtn { padding: 6px 10px; border: 1px solid #999; border-radius: 8px; background: #fff; cursor: pointer; min-width: 36px; }
.pbtn.active { border-color: #111; font-weight: 700; }
</style>
