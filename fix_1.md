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

