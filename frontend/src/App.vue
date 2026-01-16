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

      <button class="btn2" @click="checkHealth" :disabled="loadingHealth">Health</button>
      <span class="muted" v-if="health">{{ health }}</span>
    </div>

    <div v-if="error" class="error">{{ error }}</div>

    <div v-if="meta" class="meta">
      <span><b>Total hits:</b> {{ meta.total_hits }}</span>
      <span><b>Took:</b> {{ meta.took_ms }} ms</span>
      <span><b>Showing:</b> {{ results.length }}</span>
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
          <span class="tag">score: {{ r.score.toFixed(3) }}</span>
          <span class="tag">id: {{ r.doc_id }}</span>
          <span class="tag" v-if="r.timestamp">time: {{ r.timestamp }}</span>
          <span class="tag">lang: {{ r.lang }}</span>
        </div>
      </li>
    </ol>
  </div>
</template>

<script>
export default {
  data() {
    return {
      query: 'live indexing bm25',
      usePrf: false,
      lang: 'en',
      loading: false,
      loadingHealth: false,
      error: '',
      results: [],
      meta: null,
      health: ''
    }
  },
  methods: {
    async doSearch() {
      this.error = ''
      this.loading = true
      this.results = []
      this.meta = null
      try {
        const payload = {
          query: this.query,
          top_k: 10,
          use_prf: this.usePrf,
          filters: this.lang ? { lang: this.lang } : null
        }
        const res = await fetch('/search', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        })
        if (!res.ok) {
          const txt = await res.text()
          throw new Error(`Search failed (${res.status}): ${txt}`)
        }
        const data = await res.json()
        this.results = data.results || []
        this.meta = { total_hits: data.total_hits, took_ms: data.took_ms }
      } catch (e) {
        this.error = e?.message || String(e)
      } finally {
        this.loading = false
      }
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
.meta { display: flex; gap: 18px; font-size: 13px; margin: 10px 0; }
.results { padding-left: 18px; }
.item { margin: 14px 0; }
.title { font-weight: 600; margin-bottom: 6px; }
.title a { color: inherit; text-decoration: underline; }
.snippet { color: #333; font-size: 13px; line-height: 1.35; }
.info { margin-top: 6px; display: flex; gap: 8px; flex-wrap: wrap; }
.tag { font-size: 12px; border: 1px solid #ddd; padding: 2px 6px; border-radius: 999px; color: #444; }
.muted { color: #666; font-size: 13px; }
.error { background: #fff3f3; border: 1px solid #ffd0d0; padding: 10px 12px; border-radius: 8px; color: #a10000; margin: 10px 0; }
</style>
