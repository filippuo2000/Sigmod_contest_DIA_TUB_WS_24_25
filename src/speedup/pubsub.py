import copy

from src.common_functionality.distances import DistanceType
from src.common_functionality.documents import Document, DocumentCollection
from src.common_functionality.errors import ErrorCode
from src.speedup.pubsub_functionality import (
    ProcessedTopicsCache,
    ResultCollector,
    Subscriber,
    SubscriberManager,
    TopicManager,
    get_results,
)


class PubSubVersion:
    @staticmethod
    def MatchDocument(id: int, doc_words: frozenset[str]):
        doc_words = frozenset(doc_words)
        # generate a unique hash for every doc and set of queries to allow caching
        doc_hash = hash(doc_words)
        query_hash = hash(tuple(SubscriberManager.get_active_subscribers()))

        # check if results for a combination of document and queries 
        # have already been obtained
        results = get_results(doc_hash, query_hash)
        if results:
            DocumentCollection.add_document(id, Document(id, sorted(results)))
            return ErrorCode.EC_SUCCESS

        cached_topics = ProcessedTopicsCache.get_doc(doc_hash)
        curr_topic_ids = TopicManager.get_active_topics_keys()

        if cached_topics:
            cached_negative_topics = cached_topics[0]
            cached_positive_topics = cached_topics[1]

            if cached_negative_topics:
                curr_negative_ids = curr_topic_ids.intersection(
                    cached_negative_topics
                )
            if cached_positive_topics:
                curr_positive_ids = curr_topic_ids.intersection(
                    cached_positive_topics
                )

            # obtain Topic objects for given ids
            curr_positive_topics = TopicManager.get_topics_by_keys(
                curr_positive_ids
            )

            # inform subscribers about the match
            for topic in curr_positive_topics:
                topic.matched()

            # this means all current topics have been covered before
            if len(curr_topic_ids) == len(curr_positive_ids) + len(
                curr_negative_ids
            ):
                results = ResultCollector.get_results()
                SubscriberManager.reset_subscriber_count()
                DocumentCollection.add_document(
                    id, Document(id, sorted(results))
                )
                ResultCollector.reset_results()
                return ErrorCode.EC_SUCCESS
            else:
                # process only unprocessed topics for a given doc
                curr_topic_ids = (
                    curr_topic_ids - curr_positive_ids - curr_negative_ids
                )
                topics = TopicManager.get_topics_by_keys(curr_topic_ids)
        else:
            topics = set(TopicManager.get_active_topics().values())

        # match active, unprocessed earlier topics
        topics_to_shut = set()
        for word in doc_words:
            for topic in topics:
                if topic.receive(word):
                    topic.matched()
                    topics_to_shut.add(topic)
            topics -= topics_to_shut
            topics_to_shut.clear()
            if topics is None:
                break

        # unmatched topics are the ones left after processing
        negative_topic_ids = set((topic.id for topic in topics))

        SubscriberManager.reset_subscriber_count()
        results = ResultCollector.get_results()
        ProcessedTopicsCache.add_doc(
            doc_hash, curr_topic_ids - negative_topic_ids, negative_topic_ids
        )
        DocumentCollection.add_document(id, Document(id, sorted(results)))
        ResultCollector.reset_results()
        return ErrorCode.EC_SUCCESS

    @staticmethod
    def StartQuery(
        id: int, dist_type: int, words: list[str], tolerance: int = None
    ):
        if not words:
            raise ValueError(
                f"empty word list for query id {id}, words are {words}"
            )
        SubscriberManager.add_subscriber(
            id, Subscriber(id, DistanceType(dist_type), words, tolerance)
        )
        return ErrorCode.EC_SUCCESS

    @staticmethod
    def EndQuery(id: int):
        SubscriberManager.remove_subscriber(id)
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
