
> 本文档用于明确 **系统各模块的职责边界、输入输出接口以及模块负责人**，  
> 目标是支持 **并行开发、减少协作冲突、明确个人贡献范围**。

---

---

## 1. 全系统统一数据结构（唯一标准）

**文件位置：** `backend/app/schemas.py`  

系统中所有模块 **必须使用以下数据结构** 进行交互。

---

### 1.1 文档结构（Document）

```python
Document(
  doc_id: str,
  title: str,
  body: str,
  url: Optional[str],
  timestamp: Optional[str],  # ISO 8601 格式
  lang: str = "en"
)
````

* 系统中唯一合法的文档表示形式
* 禁止使用 dict / tuple 等替代格式在模块间传递

---

### 1.2 搜索请求（SearchRequest）

```json
{
  "query": "string",
  "top_k": 10,
  "use_prf": false,
  "filters": {
    "lang": "en",
    "time_from": null,
    "time_to": null,
    "field": null
  }
}
```

---

### 1.3 搜索结果（SearchResponse）

```json
{
  "query": "string",
  "took_ms": 15,
  "total_hits": 38,
  "results": [
    {
      "doc_id": "doc-1",
      "title": "标题",
      "snippet": "文本摘要...",
      "score": 1.234,
      "url": "...",
      "timestamp": "...",
      "lang": "en"
    }
  ]
}
```

---

## 2. 各模块职责与接口定义

---

## 2.1 前端界面与系统整合模块

**负责成员：** 负责前端与整体系统整合的成员

**相关文件：**

* `frontend/src/*`
* `backend/app/main.py`
* `backend/app/schemas.py`

### 输入

* 用户输入（查询文本、PRF 开关、过滤条件）
* 后端 API 返回的 JSON 数据

### 输出

* 向后端发送 `SearchRequest`
* 将 `SearchResponse` 渲染为搜索结果页面

### 职责说明

* 定义并维护前后端 API 接口
* 保证前端只依赖接口，不依赖后端内部实现
* 完成系统整体集成与部署

### 不在职责范围内

* 检索模型设计
* 索引结构实现
* 数据爬取逻辑

---

## 2.2 索引与存储模块

**负责成员：** 负责索引构建与存储的成员

**相关文件：**

* `storage/document_store.py`
* `storage/index_store.py`
* `indexing/indexer.py`
* `indexing/tokenizer.py`

### 输入

* 来自数据采集模块的 `List[Document]`
* 索引构建调用：

  * `build_index()`
  * `update_index(new_docs)`

### 输出

* 持久化文档存储（如 `docs.jsonl` 或数据库）
* 持久化倒排索引文件
* `IndexStore` 对象（供检索模块只读使用）

### 职责说明

* 文档结构规范化与校验
* 索引构建、更新与落盘
* 支持增量索引更新（live indexing）

### 不在职责范围内

* 查询解析
* 文档排序与评分
* 前端与 API 逻辑

---

## 2.3 查询解析与检索模块

**负责成员：** 负责查询处理与排序的成员

**相关文件：**

* `search/query_parser.py`
* `search/bm25.py`
* `search/searcher.py`

### 输入

* `SearchRequest`
* `DocumentStore`（只读）
* `IndexStore`（只读）

### 输出

* `SearchResponse`
* （可选）Top-k 文档 ID（供 PRF 使用）

### 职责说明

* 查询解析（普通查询 / 短语查询 / 布尔查询 / 字段查询）
* 排序模型实现（BM25 或等价模型）
* 构建搜索结果（snippet、score 等）

### 不在职责范围内

* 索引写入
* 数据采集
* 前端展示

---

## 2.4 数据采集与动态更新模块

**负责成员：** 负责数据抓取与动态更新的成员

**相关文件：**

* `ingest/crawler_runner.py`
* `ingest/scheduler.py`

### 输入

* 外部公开数据源（网站 / 数据集）
* 定时或手动触发信号

### 输出

* 标准化后的 `List[Document]`
* 调用以下任一方式完成写入：

  * `DocumentStore.add_documents()`
  * `update_index(new_docs)`
  * `/admin/ingest`

### 职责说明

* 数据抓取与清洗
* 文档格式标准化
* 演示“新增数据可被实时搜索”的流程

### 不在职责范围内

* 排序算法
* 查询逻辑
* UI 开发

---

## 2.5 高级特性模块（PRF / Query Expansion）

**负责成员：** 负责高级特性与报告撰写的成员

**相关文件：**

* `features/prf.py`
* `report/*`

### 输入

* 原始查询文本
* Top-ranked 文档 ID 列表
* `DocumentStore`（只读）

### 输出

* 扩展后的查询词或查询
* 用于报告的对比示例与分析结果

### 职责说明

* 实现一个可插拔的高级检索特性
* 与基础检索逻辑解耦
* 提供 baseline 对比说明

### 不在职责范围内

* 主检索流程修改
* 索引结构调整
* API 接口变更

---

## 3. API 接口（稳定）

### 3.1 搜索接口

```
POST /search
```

* 输入：`SearchRequest`
* 输出：`SearchResponse`

---

### 3.2 数据写入接口（内部）

```
POST /admin/ingest
```

* 输入：`List[Document]`
* 输出：写入状态与索引版本号

---

### 3.3 系统状态接口

```
GET /health
```

* 返回系统运行状态、索引版本、文档数量

---


所有搜索逻辑必须通过 `searcher.search()`
模块之间禁止直接访问彼此内部状态
文档只能以 `Document` 结构在模块间流动

---




