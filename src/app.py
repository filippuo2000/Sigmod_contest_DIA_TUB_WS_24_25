import copy
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum

import Levenshtein


class DistanceType(Enum):
    NORMAL = 0
    HAMMING = 1
    THIRD = 2

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}.{self.name}"


@dataclass
class Query:
    id: int
    dist_type: DistanceType  # should be enum
    keywords: list[str]
    tolerance: int = None

    def __repr__(self) -> str:
        class_name = self.__class__.__name__
        return (
            f"Class {class_name} with attributes, query_id: {self.id},"
            f"dist_type: {self.dist_type}, keywords: {self.keywords},"
            f"tolerance: {self.tolerance}"
        )


@dataclass
class Document:
    id: int
    num_results: int
    query_ids: list[int]


class DocumentCollection:
    docs = defaultdict(Document)

    @staticmethod
    def add_document(doc_id: int, document: Document):
        DocumentCollection.docs[doc_id] = document

    @staticmethod
    def get_doc_results(doc_id: int):
        return DocumentCollection.docs[doc_id]

    @staticmethod
    def remove_doc(doc_id: int):
        if doc_id in DocumentCollection.docs:
            del DocumentCollection.docs[doc_id]


class QueryManager:
    active_queries = defaultdict(Query)

    @staticmethod
    def add_query(query_id: int, query_object: Query):
        QueryManager.active_queries[query_id] = query_object

    @staticmethod
    def remove_query(query_id):
        if query_id in QueryManager.active_queries:
            del QueryManager.active_queries[query_id]

    @staticmethod
    def get_active_queries():
        return QueryManager.active_queries


def calc_normal(query_word: str, doc_word: str, _):
    return query_word == doc_word


def calc_hamming(query_word: str, doc_word: str, tolerance: int = None):
    if len(query_word) == len(doc_word):
        return (
            sum(c1 != c2 for c1, c2 in zip(query_word, doc_word)) <= tolerance
        )
    else:
        return False


def calc_third(query_word: str, doc_word: str, tolerance: int = None):
    return Levenshtein.distance(query_word, doc_word) <= tolerance


def CalcMatch(
    match_type: DistanceType,
    query_words: list[str],
    doc_word: str,
    tolerance: int = None,
) -> bool:
    if query_words:
        calc_match = {
            DistanceType.NORMAL: calc_normal,
            DistanceType.HAMMING: calc_hamming,
            DistanceType.THIRD: calc_third,
        }
        words_to_remove = []
        for query_word in query_words:
            if calc_match[match_type](query_word, doc_word, tolerance):
                words_to_remove.append(query_word)
                # query_words.remove(query_word)
        [query_words.remove(word) for word in words_to_remove]

    else:
        raise ValueError(
            "query_words list should not be empty when calculating distance"
        )


def get_queries():
    return QueryManager.get_active_queries()


def StartQuery(
    id: int, dist_type: int, words: list[str], tolerance: int = None
) -> None:
    if not words:
        raise ValueError(
            f"empty word list for query id {id}, words are {words}"
        )
    QueryManager.add_query(
        id, Query(id, DistanceType(dist_type), words, tolerance)
    )


def EndQuery(id: int):
    QueryManager.remove_query(id)


def MatchDocument(id: int, doc_words: list[str]):
    queries = copy.deepcopy(QueryManager.get_active_queries())
    res_queries = []
    for q_id, query in queries.items():
        for word in doc_words:
            CalcMatch(query.dist_type, query.keywords, word, query.tolerance)
            if not query.keywords:
                res_queries.append(q_id)
                break
    DocumentCollection.add_document(
        id, Document(id, len(res_queries), sorted(res_queries))
    )


def GenNextAvailableRes(res_id: int):
    results = copy.deepcopy(DocumentCollection.get_doc_results(res_id))
    assert results.id == res_id
    DocumentCollection.remove_doc(res_id)
    return results.num_results, results.query_ids
