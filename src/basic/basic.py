import copy

from src.basic.query_manager import QueryManager
from src.basic.utils import CalcMatch
from src.common_functionality.distances import DistanceType
from src.common_functionality.documents import Document, DocumentCollection
from src.common_functionality.errors import ErrorCode
from src.common_functionality.queries import Query


class BasicVersion:
    @staticmethod
    def StartQuery(
        id: int, dist_type: int, words: list[str], tolerance: int = None
    ):
        if not words:
            raise ValueError(
                f"empty word list for query id {id}, words are {words}"
            )
        QueryManager.add_query(
            id, Query(id, DistanceType(dist_type), words, tolerance)
        )
        return ErrorCode.EC_SUCCESS

    @staticmethod
    def EndQuery(id: int):
        QueryManager.remove_query(id)
        return ErrorCode.EC_SUCCESS

    @staticmethod
    def MatchDocument(id: int, doc_words: list[str]):
        queries = copy.deepcopy(QueryManager.get_active_queries())
        res_queries = []
        for q_id, query in queries.items():
            for word in doc_words:
                CalcMatch(
                    query.dist_type, query.keywords, word, query.tolerance
                )
                # list of query words for a query is modified inside the CalcMatch
                if not query.keywords:
                    res_queries.append(q_id)
                    break
        DocumentCollection.add_document(id, Document(id, sorted(res_queries)))
        return ErrorCode.EC_SUCCESS

    @staticmethod
    def GenNextAvailableRes(res_id: int, empty_list: list):
        if DocumentCollection.get_doc_results == 0:
            return ErrorCode.EC_NO_AVAIL_RES
        results = copy.deepcopy(DocumentCollection.get_doc_results(res_id))
        assert results.id == res_id
        empty_list.extend(results.query_ids)
        DocumentCollection.remove_doc(res_id)
        return ErrorCode.EC_SUCCESS
