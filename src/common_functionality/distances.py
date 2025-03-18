from enum import Enum

import Levenshtein


class DistanceType(Enum):
    NORMAL = 0
    HAMMING = 1
    THIRD = 2

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}.{self.name}"


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
