<template>
  <!-- Splash / initial screen -->
  <div v-if="!started" class="splash">
    <div class="splash-center">
      <h1 class="splash-title">Personal Research Digest Search Engine</h1>

      <form class="splash-search" @submit.prevent="startSearch">
        <input
          v-model="query"
          class="splash-q"
          placeholder=''
          autocomplete="off"
        />
        <button class="btn" :disabled="!query.trim()">Search</button>
      </form>
    </div>

    <div class="splash-help">
      <div class="usage-notes">
        <h3>Usage notes</h3>
        <ul>
          <li>Term queries: python, machine_learning</li>
          <li>Phrase queries: "machine learning"</li>
          <li>Proximity queries: #5(artificial, intelligence)</li>
          <li>Boolean operators: AND, OR, NOT</li>
          <li>Parenthetical grouping: (python OR java) AND "web development"</li>
        </ul>
      </div>
    </div>
  </div>

  <!-- Existing UI (shown after first search) -->
  <div v-else class="wrap">
    <div class="top-fixed">
      <h1>Personal Research Digest Search Engine</h1>

    <form class="search" @submit.prevent="doSearch">
      <input
        v-model="query"
        class="q"
        placeholder='Type to search'
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
        Sort:
        <select v-model="sortMode">
          <option value="relevance">Relevance</option>
          <option value="freshness">Freshness-boosted</option>
        </select>
      </label>

      <label class="sel">
        From:
        <input type="date" v-model="dateFrom" />
      </label>

      <label class="sel">
        To:
        <input type="date" v-model="dateTo" />
      </label>

      <label class="sel">
        Page size:
        <select v-model.number="pageSize">
          <option :value="5">5</option>
          <option :value="10">10</option>
          <option :value="20">20</option>
        </select>
      </label>

<!--      <button class="btn2" @click="checkHealth" :disabled="loadingHealth">Health</button>-->
<!--      <span class="muted" v-if="health">{{ health }}</span>-->
    </div>

    <div v-if="error" class="error">{{ error }}</div>

    <div v-if="meta" class="meta">
      <span><b>Total hits:</b> {{ meta.total_hits }}</span>
      <span><b>Current page took:</b> {{ formatMs(currentPageTookMs) }}</span>
      <span><b>Prefetch took:</b> {{ formatMs(prefetchTookMs) }}</span>
      <span><b>Showing:</b> {{ results.length }}</span>
      <span v-if="paginationReady" class="muted">page {{ currentPage }}</span>
    </div>

    <div v-if="loading" class="muted">Searching...</div>
    <div v-else-if="results.length === 0 && meta" class="muted">No results.</div>

    </div>

    <ul class="results" v-if="results.length">
      <li v-for="r in results" :key="r.doc_id" class="item">
        <div class="title">
          <a v-if="r.url" :href="r.url" target="_blank" rel="noreferrer" v-html="highlight(r.title)"></a>
          <span v-else v-html="highlight(r.title)"></span>
        </div>
        <div class="snippet" v-html="highlight(r.snippet)"></div>
        <div class="info">
          <span class="tag">score: {{ formatScore(r.score) }}</span>
          <span class="tag">id: {{ r.doc_id }}</span>
          <span class="tag" v-if="r.timestamp">time: {{ r.timestamp }}</span>
          <span class="tag">lang: {{ r.lang }}</span>
        </div>
      </li>
    </ul>

    <!-- Pagination (max 5 pages, current page as centered as possible) -->
    <div v-if="paginationReady" class="pager">
      <button class="btn2" :disabled="currentPage <= 1" @click="goToPage(currentPage - 1)">Prev</button>

      <button
        v-for="p in pageButtons"
        :key="`p-${p}`"
        class="pbtn"
        :class="{ active: p === currentPage, ellipsis: p === '…' }"
        :disabled="p === '…'"
        @click="p === '…' ? null : goToPage(p)"
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
      // splash state
      started: false,

      // search state
      query: '',
      lastCommittedQuery: '',

      usePrf: false,
      lang: 'en',
      sortMode: 'relevance',
      dateFrom: '',
      dateTo: '',
      pageSize: 10,

      loading: false,
      // loadingHealth: false,
      prefetching: false,
      error: '',

      results: [],
      meta: null,
      // health: '',

      // timing (ms, from backend took_ms)
      currentPageTookMs: null, // only for fetching 1 page; 0 when using cache
      prefetchTookMs: null, // last prefetch (try 5 pages)

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
    pageButtons() {
      const btns = []
      if (!this.meta) return btns

      const start = this.windowStart
      const end = this.windowEnd

      // From page 4 onward: force show "1" so user can jump back quickly
      const shouldShowFirst = this.currentPage >= 4 && start > 1 && !!this.pageCache[1]

      if (shouldShowFirst) {
        btns.push(1)
        // If there's a gap of 2+ pages, insert an ellipsis
        if (start > 2) btns.push('…')
      }

      for (let p = start; p <= end; p++) btns.push(p)
      return btns
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
      if (!this.started) return
      const q = (this.lastCommittedQuery || this.query || '').trim()
      if (!q) return
      this.query = q
      this.doSearch()
    },
    // changing filters/sort should refresh results (and reset pagination cache)
    sortMode() {
      if (!this.started) return
      const q = (this.lastCommittedQuery || this.query || '').trim()
      if (!q) return
      this.query = q
      this.doSearch()
    },
    dateFrom() {
      if (!this.started) return
      const q = (this.lastCommittedQuery || this.query || '').trim()
      if (!q) return
      this.query = q
      this.doSearch()
    },
    dateTo() {
      if (!this.started) return
      const q = (this.lastCommittedQuery || this.query || '').trim()
      if (!q) return
      this.query = q
      this.doSearch()
    }
  },
  methods: {
    startSearch() {
      const q = this.query.trim()
      if (!q) return
      this.started = true
      // After entering the main view, run health + first search
      // this.checkHealth()
      this.doSearch()
    },

    formatScore(s) {
      const n = Number(s)
      return Number.isFinite(n) ? n.toFixed(3) : String(s)
    },

    formatMs(ms) {
      if (ms === 0) return '0 ms'
      const n = Number(ms)
      return Number.isFinite(n) ? `${n} ms` : '-'
    },

    highlight(text) {
      // Highlight query terms (and quoted phrases) in a safe way.
      if (text == null) return ''
      const raw = String(text)
      const q = (this.lastCommittedQuery || this.query || '').trim()
      if (!q) return this.escapeHtml(raw)

      // Extract quoted phrases: "foo bar"
      const phrases = []
      const quoteRe = /"([^"]+)"/g
      let m
      while ((m = quoteRe.exec(q))) {
        const phrase = (m[1] || '').trim()
        if (phrase) phrases.push(phrase)
      }

      // Remaining tokens (remove quoted parts first)
      const unquoted = q.replace(quoteRe, ' ')
      const tokens = unquoted
        .split(/\s+/)
        .map((t) => t.trim())
        .filter(Boolean)
        // Drop common boolean/operators users might type
        .filter((t) => !/^(and|or|not)$/i.test(t))

      // Build unique term list, longest first (so phrases win)
      const terms = Array.from(new Set([...phrases, ...tokens]))
        .filter((t) => t.length >= 2)
        .sort((a, b) => b.length - a.length)

      if (terms.length === 0) return this.escapeHtml(raw)

      const escapedRaw = this.escapeHtml(raw)

      // Escape regex metacharacters inside terms
      const esc = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
      const pattern = terms.map(esc).join('|')
      const re = new RegExp(`(${pattern})`, 'gi')

      return escapedRaw.replace(re, '<strong>$1</strong>')
    },

    escapeHtml(str) {
      return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;')
    },

    buildPayload({ size, cursor }) {
      return {
        query: this.lastCommittedQuery || this.query,
        size,
        top_k: size, // for backward compatibility, backend should ignore if both provided //----------------------------------------------------------------------------------------------------------------------
        use_prf: this.usePrf,
        last_min_bm25_score: cursor?.lastscore ?? null,
        last_max_rerank_id: cursor?.lastid ?? null,
        filters: {
          lang: this.lang || null,
          time_from: this.dateFrom ? `${this.dateFrom}T00:00:00Z` : null,
          time_to: this.dateTo ? `${this.dateTo}T23:59:59Z` : null,
          field: null,
          sort: this.sortMode || 'relevance'
        }
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

      // Remember the last query actually executed.
      this.lastCommittedQuery = q

      this.error = ''
      this.loading = true
      this.prefetching = false
      this.results = []
      this.meta = null
      this.currentPageTookMs = null
      this.prefetchTookMs = null
      this.resetPaginationState()

      try {
        // first page: size=pageSize
        const data = await this.callSearchApi({ size: this.pageSize, cursor: null })
        const page1 = data.results || []

        this.pageCache[1] = page1
        this.pageEndCursor[1] = this.getEndCursorFromResults(page1)

        this.results = page1
        this.meta = { total_hits: data.total_hits }
        this.currentPageTookMs = data.took_ms

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
        this.prefetchTookMs = data.took_ms
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

      // If the input box was cleared/edited without re-searching, keep using the last executed query.
      // Also restore it back into the input so UI matches the results/pages.
      if (this.lastCommittedQuery && this.query.trim() !== this.lastCommittedQuery) {
        this.query = this.lastCommittedQuery
      }

      // show cached page immediately
      const cached = this.pageCache[p]
      if (cached) {
        this.currentPage = p
        this.results = cached
        this.currentPageTookMs = 0
      } else {
        // This should be rare because we only render page buttons for cached pages,
        // but we keep a fallback.
        this.loading = true
        try {
          const cursor = p === 1 ? null : await this.ensureCursorForPageEnd(p - 1)
          const data = await this.callSearchApi({ size: this.pageSize, cursor })
          const items = data.results || []
          this.currentPageTookMs = data.took_ms
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

    // async checkHealth() {
    //   this.health = ''
    //   this.loadingHealth = true
    //   try {
    //     const res = await fetch('/health')
    //     const data = await res.json()
    //     this.health = `status=${data.status}, docs=${data.docs_count}, index=${data.index_version}`
    //   } catch (e) {
    //     this.health = 'health check failed'
    //   } finally {
    //     this.loadingHealth = false
    //   }
    // }
  }
}
</script>

<style>
/* Splash */
.splash {
  height: 90vh;
  overflow: hidden;
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
  padding: 18px;
  font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
}

.splash-center {
  width: min(720px, 100%);
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 14px;
  margin-bottom: 18vh;
}

.splash-title {
  font-size: 26px;
  margin: 0;
}

.splash-search {
  width: 100%;
  display: flex;
  gap: 10px;
}

.splash-q {
  flex: 1;
  padding: 12px 14px;
  font-size: 15px;
  border: 1px solid #ccc;
  border-radius: 10px;
}

.splash-help {
  position: fixed;
  left: 50%;
  transform: translateX(-50%);
  bottom: 10vh;
  width: min(720px, calc(100% - 36px));
}

.help-title {
  font-size: 13px;
  color: #666;
  margin: 0 0 8px;
}

.help-box {
  width: 100%;
  min-height: 110px;
  resize: vertical;
  padding: 12px 14px;
  border: 1px solid #ddd;
  border-radius: 12px;
  font-size: 13px;
  line-height: 1.4;
}

/* Existing UI */
.wrap { max-width: 900px; margin: 32px auto; padding: 0 16px; font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; }

/* Keeps the title + search + filters fixed at the top while scrolling results */
.top-fixed {
  position: sticky;
  top: 0;
  z-index: 20;
  background: #fff;
  padding: 16px 0 10px;
  border-bottom: 1px solid #eee;
}
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
.results li {list-style: none; }
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

.usage-notes {
  width: 70%;
  max-width: 900px;
  margin: 10px auto 0 auto;
  padding: 24px 28px;

  background: #ffffff;
  border: 1px solid #e5e7eb;
  border-radius: 14px;

  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.04);

  text-align: left;
}

.usage-notes h3 { margin-top: 0; margin-bottom: 12px; font-size: 18px; font-weight: 600; color: #333; }
.usage-notes ul { padding-left: 18px; margin: 0; }
.usage-notes li { margin-bottom: 8px; font-size: 14px; color: #555; line-height: 1.6; }

@media (orientation: landscape) and (max-height: 480px) {
  .splash {
    height: 90vh;
    overflow: auto;
    justify-content: flex-start;
    padding-top: 14px;
    padding-bottom: calc(14px + env(safe-area-inset-bottom, 0px));
  }

  .splash-center {
    margin-bottom: 12px;
  }

  .splash-help {
    position: static;
    left: auto;
    bottom: auto;
    transform: none;
    width: 100%;
    margin-top: 10px;
  }

  .usage-notes {
    max-height: 42vh;
    overflow: auto;
  }
}
</style>
