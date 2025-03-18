from functools import cache, lru_cache
from operator import itemgetter

from src.common_functionality.distances import (
    DistanceType,
    calc_hamming,
    calc_normal,
    calc_third,
)
from src.common_functionality.queries import Query


class Subscriber(Query):
    def __init__(
        self,
        id: int,
        dist_type: DistanceType,
        keywords: list[str],
        tolerance: int = None,
    ):
        super().__init__(id, dist_type, keywords, tolerance)
        self.num_words_to_satisfy: int = len(self.keywords)

    def word_satisfied(self):
        self.num_words_to_satisfy -= 1
        if self.num_words_to_satisfy == 0:
            ResultCollector.add_result(self.id)
        return None

    def reset_counter(self):
        self.num_words_to_satisfy: int = len(self.keywords)


class Topic:
    def __init__(
        self,
        word: str,
        dist_type: DistanceType,
        tolerance: int,
        subscriber: Subscriber,
    ):
        self.id = (word, dist_type, tolerance)
        self.word = word
        self.dist_type = dist_type
        self.tolerance = tolerance
        self.subscribers = {subscriber.id: subscriber}

    def receive(self, doc_word: str):
        if CalcMatch(self.dist_type, self.word, doc_word, self.tolerance):
            return True
        return False

    # inform subscribers
    def matched(self):
        for _, subscriber in self.subscribers.items():
            subscriber.word_satisfied()

    # reset internal counters for all subscribers
    def reset_subscribers(self):
        for _, subscriber in self.subscribers.items():
            subscriber.reset_counter()

    def __repr__(self) -> str:
        class_name = self.__class__.__name__
        return (
            f"Class {class_name} with attributes, id: {self.id},"
            f"word: {self.word}, dist_type: {self.dist_type},"
            f"tolerance: {self.tolerance}"
        )


class TopicManager:
    active_topics: dict[tuple[str, DistanceType, int], Topic] = {}

    @staticmethod
    def add_topic(
        topic_id: tuple[str, DistanceType, int],
        word: str,
        subscriber: Subscriber,
    ):
        if topic_id in TopicManager.active_topics:
            # add a subscriber to an existing topic
            TopicManager.active_topics[topic_id].subscribers[
                subscriber.id
            ] = subscriber
        else:
            TopicManager.active_topics[topic_id] = Topic(
                word, subscriber.dist_type, subscriber.tolerance, subscriber
            )

    @staticmethod
    def remove_subscriber(
        topic_id: tuple[str, DistanceType, int], sub_id: int
    ):
        if topic_id in TopicManager.active_topics.keys():
            del TopicManager.active_topics[topic_id].subscribers[sub_id]
            if not TopicManager.active_topics[topic_id].subscribers:
                TopicManager.remove_topic(topic_id)

    @staticmethod
    def remove_topic(topic_id: tuple[str, DistanceType, int]):
        if topic_id in TopicManager.active_topics.keys():
            del TopicManager.active_topics[topic_id]

    @staticmethod
    def get_active_topics():
        return TopicManager.active_topics

    def get_topics_by_keys(topic_ids: set[int]):
        if len(topic_ids) == 1:
            single_id = next(iter(topic_ids))
            return {TopicManager.active_topics[single_id]}
        else:
            return set(itemgetter(*topic_ids)(TopicManager.active_topics))

    @staticmethod
    def get_active_topics_keys():
        return set(TopicManager.active_topics.keys())

    @staticmethod
    def reset():
        TopicManager.active_topics = {}


class SubscriberManager:
    active_subscribers: dict[int, Subscriber] = {}

    @staticmethod
    def add_subscriber(query_id: int, query_object: Subscriber):
        SubscriberManager.active_subscribers[query_id] = query_object
        for word in query_object.keywords:
            TopicManager.add_topic(
                (word, query_object.dist_type, query_object.tolerance),
                word,
                query_object,
            )

    @staticmethod
    def remove_subscriber(query_id: int):
        if query_id in SubscriberManager.active_subscribers:
            sub = SubscriberManager.active_subscribers[query_id]
            for word in sub.keywords:
                TopicManager.remove_subscriber(
                    (word, sub.dist_type, sub.tolerance), query_id
                )
            del SubscriberManager.active_subscribers[query_id]

    @staticmethod
    def get_active_subscribers():
        return SubscriberManager.active_subscribers

    @staticmethod
    def reset_subscriber_count():
        for _, sub in SubscriberManager.active_subscribers.items():
            sub.num_words_to_satisfy = len(sub.keywords)

    @staticmethod
    def reset():
        SubscriberManager.active_subscribers = {}


class ResultCollector:
    curr_results: list[int] = []

    @staticmethod
    def add_result(res_id: int):
        ResultCollector.curr_results.append(res_id)

    @staticmethod
    def get_results():
        return ResultCollector.curr_results

    @staticmethod
    def reset_results():
        ResultCollector.curr_results = []


# pseudo-function for caching reasons only
@lru_cache(maxsize=512)
def get_results(doc_hash: int, queries_hash: int):
    return ResultCollector.get_results()


class ProcessedTopicsCache:
    doc_results: dict[dict[int, set[tuple[str, DistanceType, int]]]] = {}

    @staticmethod
    def add_doc(
        doc_hash: int,
        positive_topics_ids: set[tuple[str, DistanceType, int]],
        negative_topics_ids: set[tuple[str, DistanceType, int]],
    ):
        # delete document from cache if size exceeded
        if len(ProcessedTopicsCache.doc_results) > 5000:
            first_key = next(iter(ProcessedTopicsCache.doc_results))
            del ProcessedTopicsCache.doc_results[first_key]

        # delete topic ids for a document from cache if size exceeded
        if doc_hash in ProcessedTopicsCache.doc_results:
            if len(ProcessedTopicsCache.doc_results[doc_hash][0]) > 1000:
                ProcessedTopicsCache.doc_results[doc_hash][0].pop()
            elif len(ProcessedTopicsCache.doc_results[doc_hash][1]) > 1000:
                ProcessedTopicsCache.doc_results[doc_hash][1].pop()

            ProcessedTopicsCache.doc_results[doc_hash][0].update(
                negative_topics_ids
            )
            ProcessedTopicsCache.doc_results[doc_hash][1].update(
                positive_topics_ids
            )
        else:
            ProcessedTopicsCache.doc_results[doc_hash] = {
                0: negative_topics_ids,
                1: positive_topics_ids,
            }

    @staticmethod
    def get_doc(doc_hash: int):
        if doc_hash in ProcessedTopicsCache.doc_results:
            return ProcessedTopicsCache.doc_results[doc_hash]

    @staticmethod
    def clean_cache():
        ProcessedTopicsCache.doc_results = {}


@cache
def CalcMatch(
    dist_type: DistanceType,
    topic_word: str,
    doc_word: str,
    tolerance: int = None,
):
    calc_match = {
        DistanceType.NORMAL: calc_normal,
        DistanceType.HAMMING: calc_hamming,
        DistanceType.THIRD: calc_third,
    }
    if topic_word is not None:
        return calc_match[dist_type](topic_word, doc_word, tolerance)
    else:
        raise ValueError(
            "topic word should not be empty when calculating distance"
        )
