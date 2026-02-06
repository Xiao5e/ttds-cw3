# 文件路径: indexing/tokenizer.py

import re
from typing import List

import nltk
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords


# =========================
# 确保停用词已下载
# =========================
try:·
    nltk.data.find("corpora/stopwords")
except LookupError:
    nltk.download("stopwords")

# =========================
# 全局对象（提升性能）
# =========================
_stemmer = PorterStemmer()
_stop_words = set(stopwords.words("english"))


# =========================
# 英文分词函数
# =========================
def tokenize_en(text: str) -> List[str]:
    """
    标准分词流程：
    1. 小写化 + 正则分词
    2. 去停用词
    3. 词干提取 (stemming)
    """

    if not text:
        return []

    # 只保留字母和数字
    tokens = re.findall(r"[a-z0-9]+", text.lower())

    valid_tokens = []

    for t in tokens:
        if t not in _stop_words and len(t) > 1:
            stemmed = _stemmer.stem(t)
            valid_tokens.append(stemmed)

    return valid_tokens
