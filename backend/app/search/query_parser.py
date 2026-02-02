from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Set, Union, Optional, Tuple
import re
import math
import logging

from ..storage.index_store import IndexStore
from ..indexing.tokenizer import tokenize_en


logger = logging.getLogger(__name__)


@dataclass
class QueryNode:
    """查询语法树节点基类"""

    def evaluate(self, index: IndexStore) -> Set[str]:
        """评估节点，返回匹配的文档集合"""
        raise NotImplementedError

    def to_string(self, parent_precedence: int = 0) -> str:
        """将节点转换为字符串表示"""
        raise NotImplementedError

    def __str__(self) -> str:
        return self.to_string()


@dataclass
class TermNode(QueryNode):
    """词项节点"""
    term: str

    def evaluate(self, index: IndexStore) -> Set[str]:
        """评估词项节点"""
        if self.term in index.postings:
            return set(doc_id for doc_id, _ in index.postings[self.term])
        return set()

    def to_string(self, parent_precedence: int = 0) -> str:
        return self.term


@dataclass
class PhraseNode(QueryNode):
    """短语节点"""
    terms: List[str]

    def evaluate(self, index: IndexStore) -> Set[str]:
        """评估短语节点"""
        if not self.terms:
            return set()

        # 获取包含第一个词的所有文档作为候选
        first_term = self.terms[0]
        if first_term not in index.postings:
            return set()

        # 如果没有位置信息，退化为AND查询
        if not hasattr(index, 'positions') or not index.positions:
            result = set(doc_id for doc_id, _ in index.postings.get(first_term, []))
            for term in self.terms[1:]:
                if term not in index.postings:
                    return set()
                term_docs = set(doc_id for doc_id, _ in index.postings[term])
                result &= term_docs
            return result

        # 使用位置信息进行精确短语匹配
        candidate_docs = set(doc_id for doc_id, _ in index.postings.get(first_term, []))
        result_docs = set()

        for doc_id in candidate_docs:
            positions_lists = []
            valid = True

            for term in self.terms:
                if doc_id not in index.positions.get(term, {}):
                    valid = False
                    break
                positions = sorted(index.positions[term][doc_id])
                positions_lists.append(positions)

            if not valid:
                continue

            # 检查是否存在连续位置序列
            for start_pos in positions_lists[0]:
                match = True
                for i in range(1, len(self.terms)):
                    expected_pos = start_pos + i
                    if expected_pos not in positions_lists[i]:
                        match = False
                        break
                if match:
                    result_docs.add(doc_id)
                    break

        return result_docs

    def to_string(self, parent_precedence: int = 0) -> str:
        return f'"{" ".join(self.terms)}"'


@dataclass
class ProximityNode(QueryNode):
    """邻近查询节点"""
    term1: str
    term2: str
    distance: int

    def evaluate(self, index: IndexStore) -> Set[str]:
        """评估邻近查询节点"""
        # 如果没有位置信息，退化为AND查询
        if not hasattr(index, 'positions') or not index.positions:
            result1 = set(doc_id for doc_id, _ in index.postings.get(self.term1, []))
            result2 = set(doc_id for doc_id, _ in index.postings.get(self.term2, []))
            return result1 & result2

        result = set()

        if self.term1 not in index.positions or self.term2 not in index.positions:
            return result

        positions1 = index.positions.get(self.term1, {})
        positions2 = index.positions.get(self.term2, {})

        # 查找共同文档
        common_docs = set(positions1.keys()) & set(positions2.keys())

        for doc_id in common_docs:
            pos1 = sorted(positions1[doc_id])
            pos2 = sorted(positions2[doc_id])

            # 使用双指针法高效查找
            i, j = 0, 0
            found = False

            while i < len(pos1) and j < len(pos2):
                dist = abs(pos1[i] - pos2[j])
                if dist <= self.distance:
                    found = True
                    break

                if pos1[i] < pos2[j]:
                    i += 1
                else:
                    j += 1

            if found:
                result.add(doc_id)

        return result

    def to_string(self, parent_precedence: int = 0) -> str:
        return f'#{self.distance}({self.term1},{self.term2})'


@dataclass
class NotNode(QueryNode):
    """NOT运算符节点"""
    operand: QueryNode

    def evaluate(self, index: IndexStore) -> Set[str]:
        """评估NOT节点"""
        all_docs = set(index.doc_len.keys())
        operand_result = self.operand.evaluate(index)
        return all_docs - operand_result

    def to_string(self, parent_precedence: int = 0) -> str:
        child_str = self.operand.to_string(self.precedence())
        return f'NOT {child_str}'

    def precedence(self) -> int:
        return 3


@dataclass
class AndNode(QueryNode):
    """AND运算符节点"""
    left: QueryNode
    right: QueryNode

    def evaluate(self, index: IndexStore) -> Set[str]:
        """评估AND节点"""
        left_result = self.left.evaluate(index)
        right_result = self.right.evaluate(index)
        return left_result & right_result

    def to_string(self, parent_precedence: int = 0) -> str:
        left_str = self.left.to_string(self.precedence())
        right_str = self.right.to_string(self.precedence())
        result = f'{left_str} AND {right_str}'

        # 根据需要添加括号
        if parent_precedence > self.precedence():
            return f'({result})'
        return result

    def precedence(self) -> int:
        return 2


@dataclass
class OrNode(QueryNode):
    """OR运算符节点"""
    left: QueryNode
    right: QueryNode

    def evaluate(self, index: IndexStore) -> Set[str]:
        """评估OR节点"""
        left_result = self.left.evaluate(index)
        right_result = self.right.evaluate(index)
        return left_result | right_result

    def to_string(self, parent_precedence: int = 0) -> str:
        left_str = self.left.to_string(self.precedence())
        right_str = self.right.to_string(self.precedence())
        result = f'{left_str} OR {right_str}'

        # 根据需要添加括号
        if parent_precedence > self.precedence():
            return f'({result})'
        return result

    def precedence(self) -> int:
        return 1


class QueryParser:
    """查询解析器，支持完整查询语法"""

    def __init__(self):
        """
        初始化查询解析器
        """
        self.tokenizer = self._default_tokenizer
        self.operator_precedence = {
            'NOT': 3,
            'AND': 2,
            'OR': 1,
            '(': 0,
            ')': 0
        }

    def _default_tokenizer(self, text: str) -> List[str]:
        """默认词元化器：简单分词和标准化"""
        # 移除标点，转为小写，分割
        tokens = tokenize_en(text)
        return tokens

    def parse(self, query: str) -> QueryNode:
        """
        解析查询字符串，返回查询语法树

        Args:
            query: 查询字符串，支持以下语法：
                  - 词项: python, machine_learning
                  - 短语: "machine learning"
                  - 邻近查询: #5(artificial,intelligence)
                  - 布尔运算符: AND, OR, NOT
                  - 括号: (python OR java) AND "web development"

        Returns:
            QueryNode: 查询语法树的根节点

        Raises:
            ValueError: 查询语法错误
        """
        tokens = self._tokenize_query(query)
        return self._parse_expression(tokens)

    def _tokenize_query(self, query: str) -> List[Tuple[str, str]]:
        """
        将查询字符串转换为token流

        Returns:
            List of (token_type, token_value) pairs
        """
        # 定义token模式
        patterns = [
            ('PROXIMITY', r'#(\d+)\(([^,]+),([^)]+)\)'),
            ('PHRASE', r'"([^"]+)"'),
            ('NOT', r'\bNOT\b'),
            ('AND', r'\bAND\b'),
            ('OR', r'\bOR\b'),
            ('LPAREN', r'\('),
            ('RPAREN', r'\)'),
            ('TERM', r'[^\s()"]+')
        ]

        pattern = '|'.join(f'(?P<{name}>{regex})' for name, regex in patterns)
        tokens = []
        pos = 0

        while pos < len(query):
            # 跳过空白字符
            if query[pos].isspace():
                pos += 1
                continue

            match = re.match(pattern, query[pos:], re.IGNORECASE)
            if not match:
                raise ValueError(f'Invalid token at position {pos}: {query[pos:]}')

            for name, value in match.groupdict().items():
                if value is not None:
                    tokens.append((name, value))
                    pos += len(value)
                    break

        return tokens

    def _parse_expression(self, tokens: List[Tuple[str, str]]) -> QueryNode:
        """解析表达式，处理运算符优先级"""
        return self._parse_or_expression(tokens)

    def _parse_or_expression(self, tokens: List[Tuple[str, str]]) -> QueryNode:
        """解析OR表达式"""
        node = self._parse_and_expression(tokens)

        while tokens and tokens[0][0] == 'OR':
            tokens.pop(0)  # 消耗OR
            right = self._parse_and_expression(tokens)
            node = OrNode(node, right)

        return node

    def _parse_and_expression(self, tokens: List[Tuple[str, str]]) -> QueryNode:
        """解析AND表达式"""
        node = self._parse_not_expression(tokens)

        while tokens and tokens[0][0] == 'AND':
            tokens.pop(0)  # 消耗AND
            right = self._parse_not_expression(tokens)
            node = AndNode(node, right)

        return node

    def _parse_not_expression(self, tokens: List[Tuple[str, str]]) -> QueryNode:
        """解析NOT表达式"""
        if tokens and tokens[0][0] == 'NOT':
            tokens.pop(0)  # 消耗NOT
            operand = self._parse_not_expression(tokens)
            return NotNode(operand)

        return self._parse_primary(tokens)

    def _parse_primary(self, tokens: List[Tuple[str, str]]) -> QueryNode:
        """解析基本表达式"""
        if not tokens:
            raise ValueError("Unexpected end of query")

        token_type, token_value = tokens.pop(0)

        if token_type == 'LPAREN':
            node = self._parse_expression(tokens)

            if not tokens or tokens[0][0] != 'RPAREN':
                raise ValueError("Missing closing parenthesis")
            tokens.pop(0)  # 消耗RPAREN

            return node

        elif token_type == 'PHRASE':
            # 处理短语
            terms = self.tokenizer(token_value)
            return PhraseNode(terms)

        elif token_type == 'PROXIMITY':
            # 处理邻近查询 #distance(term1,term2)
            match = re.match(r'#(\d+)\(([^,]+),([^)]+)\)', token_value)
            if not match:
                raise ValueError(f"Invalid proximity format: {token_value}")

            distance = int(match.group(1))
            term1 = match.group(2).strip()
            term2 = match.group(3).strip()

            # 对词项进行标准化
            term1_tokens = self.tokenizer(term1)
            term2_tokens = self.tokenizer(term2)

            if not term1_tokens or not term2_tokens:
                raise ValueError(f"Invalid terms in proximity query: {token_value}")

            return ProximityNode(term1_tokens[0], term2_tokens[0], distance)

        elif token_type == 'TERM':
            # 处理普通词项
            term_tokens = self.tokenizer(token_value)
            if not term_tokens:
                raise ValueError(f"Invalid term: {token_value}")
            return TermNode(term_tokens[0])

        else:
            raise ValueError(f"Unexpected token: {token_type} '{token_value}'")

    def analyze_query(self, query: str) -> Dict:
        """分析查询的统计信息"""
        try:
            ast = self.parse(query)
            return self._analyze_node(ast)
        except Exception as e:
            return {
                'valid': False,
                'error': str(e),
                'complexity': 'invalid'
            }

    def _analyze_node(self, node: QueryNode) -> Dict:
        """递归分析查询节点"""
        stats = {
            'valid': True,
            'node_type': type(node).__name__,
            'term_count': 0,
            'operator_count': 0,
            'has_phrases': False,
            'has_proximity': False,
            'max_depth': 1
        }

        if isinstance(node, TermNode):
            stats['term_count'] = 1

        elif isinstance(node, PhraseNode):
            stats['term_count'] = len(node.terms)
            stats['has_phrases'] = True

        elif isinstance(node, ProximityNode):
            stats['term_count'] = 2
            stats['has_proximity'] = True
            stats['distance'] = node.distance

        elif isinstance(node, NotNode):
            stats['operator_count'] = 1
            child_stats = self._analyze_node(node.operand)
            self._merge_stats(stats, child_stats)
            stats['max_depth'] = child_stats['max_depth'] + 1

        elif isinstance(node, (AndNode, OrNode)):
            stats['operator_count'] = 1
            left_stats = self._analyze_node(node.left)
            right_stats = self._analyze_node(node.right)

            self._merge_stats(stats, left_stats)
            self._merge_stats(stats, right_stats)
            stats['max_depth'] = max(left_stats['max_depth'], right_stats['max_depth']) + 1

        # 计算复杂度
        if stats['operator_count'] > 0 or stats['has_phrases'] or stats['has_proximity']:
            stats['complexity'] = 'complex'
        else:
            stats['complexity'] = 'simple'

        return stats

    def _merge_stats(self, target: Dict, source: Dict) -> None:
        """合并统计信息"""
        target['term_count'] += source['term_count']
        target['operator_count'] += source['operator_count']
        target['has_phrases'] = target['has_phrases'] or source['has_phrases']
        target['has_proximity'] = target['has_proximity'] or source['has_proximity']
        target['max_depth'] = max(target.get('max_depth', 1), source['max_depth'])


def parse_query(query: str, index: IndexStore) -> Set[str]:
    """
    统一的查询解析接口

    Args:
        query: 查询字符串
        index: 索引存储

    Returns:
        匹配的文档ID集合

    Example:
        >>> parse_query('python AND "machine learning"', index)
        {'doc1', 'doc2', ...}

        >>> parse_query('#5(artificial,intelligence) OR AI', index)
        {'doc3', 'doc4', ...}

        >>> parse_query('(python OR java) AND NOT "web development"', index)
        {'doc5', 'doc6', ...}
    """
    parser = QueryParser()

    try:
        # 构建查询语法树
        ast = parser.parse(query)

        # 评估查询
        result = ast.evaluate(index)

        return result

    except ValueError as e:
        # 查询语法错误，返回空结果
        logger.warning(f"Query parsing error: {e} (query: '{query}')")
        return set()

    except Exception as e:
        # 其他错误
        logger.error(f"Error evaluating query '{query}': {e}", exc_info=True)
        return set()


def analyze_query(query: str) -> Dict:
    """
    分析查询字符串

    Args:
        query: 查询字符串

    Returns:
        查询分析结果
    """
    parser = QueryParser()
    return parser.analyze_query(query)
