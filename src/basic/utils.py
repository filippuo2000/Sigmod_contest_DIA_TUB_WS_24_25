from src.common_functionality.distances import (
    DistanceType,
    calc_hamming,
    calc_normal,
    calc_third,
)


def CalcMatch(
    match_type: DistanceType,
    query_words: list[str],
    doc_word: str,
    tolerance: int = None,
):
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
        [query_words.remove(word) for word in words_to_remove]

    else:
        raise ValueError(
            "query_words list should not be empty when calculating distance"
        )
