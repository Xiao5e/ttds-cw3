A. DocumentStore 实现缺失（后端会直接 import 崩）

backend/app/storage/document_store.py 文件内容看起来是 tokenizer（而不是 DocumentStore 类），并且我在整个 backend/app 里没搜到 class DocumentStore。
但 main.py 在 import DocumentStore，所以如果按 repo 现状从零启动，会直接报错。

你们要做的：把 DocumentStore 类补回来（至少要支持 add_documents() / get() / all() / load_if_exists() / persist 这套），并确认跟 IndexStore 的 doc_id 体系一致（字符串 doc_id vs 内部 int id 不能混用）。

B. import 路径混乱：indexer.py 和 main.py 对不上

main.py 用的是包内导入：from .indexing.indexer import build_index, update_index
但 backend/app/indexing/indexer.py 里实际上是 class Indexer，而且还在用类似 from schemas import Document 这种非包相对导入。这会导致：

build_index / update_index 根本不存在（启动就炸）

即使你补函数，导入路径也会在不同环境下行为不一致

你们要做的：统一一种风格（建议全部用 from ..schemas import ... 这种包相对导入），并把 build_index(store_docs, INDEX)、update_index(new_docs, INDEX) 这两个函数落实好。

C. tokenizer 文件里有非法字符，可能直接 SyntaxError

backend/app/indexing/tokenizer.py 里 try: 后面跟了一个奇怪的字符（看起来像 ·），这在某些环境会直接 语法错误。

你们要做的：把这个不可见字符删掉；并且别在 import 时自动 nltk.download()（现场 demo 没网/慢网会翻车），更稳的方式是：

依赖里带好 stopwords，或

提供 fallback：找不到 stopwords 就用一个最小停用词表。

D. 搜索结果打分字段有明显 bug（会返回错误 score）

searcher.py 在最终组装 SearchResult 时写了 score=float(score)，但那个 score 变量在该作用域并不是当前 doc 的分数（你们在 heap 里 pop 出来的是 neg_score）。这会导致返回的 score 不可信甚至错乱。

你们要做的：用 score = -neg_score，再写入 SearchResult(score=score)。


1) 修复 DocumentStore（原文件内容其实是 tokenizer，导致 import 必炸）

重新实现了 backend/app/storage/document_store.py：

add_documents / get / all / __len__

本地持久化：data/docs.pkl

load_if_exists() 支持启动时自动加载

2) 修复 IndexStore（补齐 main.py 依赖的字段与持久化方法）

给 backend/app/storage/index_store.py 增加了：

index_version + bump_version()

load_if_exists() / persist()

默认持久化：data/index.pkl

3) 重写 indexer.py，补齐 build_index() / update_index()（main.py 直接用的）

backend/app/indexing/indexer.py 改为真正提供：

build_index(docs, index)：全量重建

update_index(new_docs, index)：增量追加（去重 doc_id）

使用包内相对导入，避免环境不同导致 import 崩溃

4) tokenizer 的非法字符 & NLTK 下载翻车风险

修复 backend/app/indexing/tokenizer.py：

删掉非法字符

不再在 import 阶段自动 nltk.download()

如果环境没有 stopwords，就自动 fallback 到一个最小停用词表（demo 更稳）

5) 修复 search 返回 score 的 bug

backend/app/search/searcher.py：

SearchResult(score=...) 改为使用 -neg_score

去掉了会污染日志的 print(...)
