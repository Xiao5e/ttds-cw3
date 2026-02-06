# 文件路径: indexer.py

import os
from collections import defaultdict
from typing import List

from schemas import Document
from storage.index_store import IndexStore
from storage.document_store import DocumentStore
from indexing.tokenizer import tokenize_en


class Indexer:
    def __init__(self, data_dir: str = "./data"):
        self.data_dir = data_dir

        # 实例化两个存储核心
        self.index_store = IndexStore()
        self.doc_store = DocumentStore()

    # =========================
    # 构建索引
    # =========================
    def add_documents(self, docs: List[Document]) -> int:
        """
        全量索引构建
        """
        next_id = len(self.index_store.doc_id_map)
        count = 0

        for doc in docs:

            # 1. 去重
            if doc.doc_id in self.index_store.doc_id_map:
                continue

            # 2. 生成内部 ID
            internal_id = next_id
            self.index_store.doc_id_map[doc.doc_id] = internal_id
            self.index_store.reverse_doc_id_map[internal_id] = doc.doc_id

            # 3. 存入 DocumentStore
            self.doc_store.add_document(internal_id, doc)

            # 4. 存入元数据
            self.index_store.doc_metadata[internal_id] = {
                "title": doc.title,
                "url": doc.url,
                "timestamp": doc.timestamp
            }

            # 5. 分词
            full_text = f"{doc.title} {doc.body}"
            tokens = tokenize_en(full_text)

            # 6. 记录文档长度
            self.index_store.doc_len[internal_id] = len(tokens)

            # 7. 统计 TF & 位置
            term_freqs = defaultdict(int)
            term_positions = defaultdict(list)

            for pos, term in enumerate(tokens):
                term_freqs[term] += 1
                term_positions[term].append(pos)

            # 8. 写入倒排索引
            for term, freq in term_freqs.items():

                if term not in self.index_store.postings:
                    self.index_store.postings[term] = []
                    self.index_store.positions[term] = {}

                self.index_store.postings[term].append(
                    (internal_id, freq)
                )

                self.index_store.positions[term][internal_id] = \
                    term_positions[term]

            next_id += 1
            count += 1

        return count

    # =========================
    # 保存索引
    # =========================
    def save(self):
        """持久化保存所有数据"""

        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)

        print(f"Saving index with {len(self.index_store.doc_id_map)} docs...")

        self.index_store.save_to_disk(
            os.path.join(self.data_dir, "index.pkl")
        )

        self.doc_store.save_to_disk(
            os.path.join(self.data_dir, "docs.pkl")
        )

        print("Save complete.")
