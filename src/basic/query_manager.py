from src.common_functionality.queries import Query


class QueryManager:
    active_queries: dict[int, Query] = {}

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
