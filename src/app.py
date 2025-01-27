import copy
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from functools import lru_cache, cache
from typing import Set, List, Tuple
from conditional_cache import lru_cache
from operator import itemgetter

import Levenshtein

class ErrorCode(Enum):
    EC_SUCCESS = 0
    EC_NO_AVAIL_RES = 1
    EC_FAIL = 2

    def __str__(self):
        return f"{self.name} (Code {self.value})"
    
class DistanceType(Enum):
    NORMAL = 0
    HAMMING = 1
    THIRD = 2

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}.{self.name}"

class Document:
    def __init__(self, id, query_ids):
        self.id: int = id
        self.query_ids: Set[int] = query_ids

class DocumentCollection:
    docs = {}

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

class Query:
    def __init__(self, id: int, dist_type: DistanceType, keywords: list[str], tolerance: int = None):
        self.id: int = id
        self.dist_type: DistanceType = dist_type # should be enum
        self.keywords: list[str] = keywords
        self.tolerance: int = tolerance

    def __repr__(self) -> str:
        class_name = self.__class__.__name__
        return (
            f"Class {class_name} with attributes, query_id: {self.id},"
            f"dist_type: {self.dist_type}, keywords: {self.keywords},"
            f"tolerance: {self.tolerance}"
        )
    
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

class Subscriber(Query):
    def __init__(self, id: int, dist_type: DistanceType, keywords: list[str], tolerance: int = None):
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
    def __init__(self, word: str, dist_type: DistanceType, tolerance: int, subscriber: Subscriber):
        self.id = (word, dist_type, tolerance)
        self.word = word
        self.dist_type = dist_type
        self.tolerance = tolerance
        self.subscribers = {subscriber.id: subscriber}

    def receive(self, doc_word):
        if CalcMatch(self.dist_type, self.word, doc_word, self.tolerance):
            return True
        return False

    def matched(self):
        for _, subscriber in self.subscribers.items():
            subscriber.word_satisfied()
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


class SubscriberManager:
    active_subscribers: dict[Subscriber] = {}

    @staticmethod
    def add_subscriber(query_id: int, query_object: Subscriber):
        SubscriberManager.active_subscribers[query_id] = query_object
        for word in query_object.keywords:
            TopicManager.add_topic((word, query_object.dist_type, query_object.tolerance), word, query_object)

    @staticmethod
    def remove_subscriber(query_id: int):
        if query_id in SubscriberManager.active_subscribers:
            sub = SubscriberManager.active_subscribers[query_id]
            for word in sub.keywords:
                TopicManager.remove_subscriber((word, sub.dist_type, sub.tolerance), query_id)
                # TopicManager.remove_topic((word, sub.dist_type, sub.tolerance))
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

class TopicManager:
    active_topics: dict[Topic] = {}

    @staticmethod
    def add_topic(topic_id: Tuple, word: str, subscriber: Subscriber):
        if topic_id in TopicManager.active_topics:
            # add a subscriber to an existing topic
            TopicManager.active_topics[topic_id].subscribers[subscriber.id] = subscriber
        else:
            TopicManager.active_topics[topic_id] = Topic(word, subscriber.dist_type, subscriber.tolerance, subscriber)

    @staticmethod
    def remove_subscriber(topic_id: Tuple, sub_id: int):
        if topic_id in TopicManager.active_topics.keys():
            del TopicManager.active_topics[topic_id].subscribers[sub_id]
            if not TopicManager.active_topics[topic_id].subscribers:
                TopicManager.remove_topic(topic_id)

    @staticmethod
    def remove_topic(topic_id: Tuple):
        if topic_id in TopicManager.active_topics.keys():
            del TopicManager.active_topics[topic_id]

    @staticmethod
    def get_active_topics():
        return TopicManager.active_topics
    
    def get_topics_by_keys(topic_ids: set[int]):
        # print(f"active topics type: {type(TopicManager.active_topics)}")
        if len(topic_ids)==1:
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


class ResultCollector:
    curr_results = []

    @staticmethod
    def add_result(res_id: int):
        ResultCollector.curr_results.append(res_id)
    
    @staticmethod
    def get_results():
        return ResultCollector.curr_results
    
    @staticmethod
    def reset_results():
        ResultCollector.curr_results = []

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

@cache
def CalcMatch(
    dist_type: DistanceType,
    topic_word: str,
    doc_word: str,
    tolerance: int = None,
) -> bool:
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

# class DocCache:
#     doc_results: dict[Tuple[int, int]: List[int]] = {}

#     @staticmethod
#     def add_elem(words_hash, queries_hash, results):
#         if words_hash not in DocCache.doc_results:
#             if len(DocCache.doc_results) > 128:
#                 first_key = next(iter(DocCache.doc_results))
#                 del DocCache.doc_results[first_key]
#                 # DocCache.doc_results.pop(next(iter(DocCache.doc_results))) 
#             DocCache.doc_results[words_hash] = {queries_hash: results}
#         else:
#             if queries_hash not in DocCache.doc_results[words_hash]:
#                 DocCache.doc_results[words_hash] = {queries_hash: results}
#     @staticmethod
#     def find_elem(words_hash, queries_hash):
#         if words_hash in DocCache.doc_results:
#             if queries_hash in DocCache.doc_results[words_hash]:
#                 # print("cache")
#                 # print(len(DocCache.doc_results[words_hash].values()))
#                 return DocCache.doc_results[words_hash][queries_hash]

class ProcessedTopicsCache:
    doc_results: dict[List] = {}

    @staticmethod
    def add_doc(doc_hash: int, positive_topics_ids: set, negative_topics_ids: set):
        if len(ProcessedTopicsCache.doc_results) > 5000:
            first_key = next(iter(ProcessedTopicsCache.doc_results))
            del ProcessedTopicsCache.doc_results[first_key]
        if doc_hash in ProcessedTopicsCache.doc_results:
            if len(ProcessedTopicsCache.doc_results[doc_hash][0]) > 1000:
                ProcessedTopicsCache.doc_results[doc_hash][0].pop()
            elif len(ProcessedTopicsCache.doc_results[doc_hash][1]) > 1000:
                ProcessedTopicsCache.doc_results[doc_hash][1].pop()

            ProcessedTopicsCache.doc_results[doc_hash][0].update(negative_topics_ids)
            ProcessedTopicsCache.doc_results[doc_hash][1].update(positive_topics_ids)
        else:
            ProcessedTopicsCache.doc_results[doc_hash] = {0: negative_topics_ids, 1: positive_topics_ids}
    
    @staticmethod
    def get_doc(doc_hash: int):
        if doc_hash in ProcessedTopicsCache.doc_results:
            return ProcessedTopicsCache.doc_results[doc_hash]
    @staticmethod
    def clean_cache():
        ProcessedTopicsCache.doc_results = {}


@lru_cache(maxsize=512)
def get_results(doc_hash, queries_hash):
    return ResultCollector.get_results()

def reset_everything():
    ProcessedTopicsCache.clean_cache()
    CalcMatch.cache_clear()
    get_results.cache_clear()
    TopicManager.reset()
    SubscriberManager.reset()

class PubSubVersion:
    @staticmethod
    def MatchDocument(id: int, doc_words: frozenset[str]):
        doc_words = frozenset(doc_words)
        doc_hash = hash(doc_words)
        query_hash = hash(tuple(SubscriberManager.get_active_subscribers()))
        results = get_results(doc_hash, query_hash)
        if results:
            print("cached")
            DocumentCollection.add_document(
            id, Document(id, sorted(results))
            )
            return ErrorCode.EC_SUCCESS
        
        cached_topics = ProcessedTopicsCache.get_doc(doc_hash)
        curr_topic_ids = TopicManager.get_active_topics_keys()

        if cached_topics:
            cached_negative_topics = cached_topics[0]
            cached_positive_topics = cached_topics[1]
            # print(f"intersection is: {len(cached_negative_topics.intersection(cached_positive_topics))}")
            if cached_negative_topics:
                curr_negative_ids = curr_topic_ids.intersection(cached_negative_topics)
            if cached_positive_topics:
                curr_positive_ids = curr_topic_ids.intersection(cached_positive_topics)
            # print(f"ids type: {type(curr_positive_ids)}")
            # print(f"curr_positive ids are: {curr_positive_ids}")
            curr_positive_topics = TopicManager.get_topics_by_keys(curr_positive_ids)
            
            # print(f"topics afterewards type: {type(curr_positive_topics)}")
            # curr_negative_topics = TopicManager.get_topics_by_keys(curr_negative_ids)


            # print(f"num of positive topics: {len(curr_positive_topics)}")
            for topic in curr_positive_topics:
                topic.matched()

            # topics = topics - curr_negative_topics - curr_positive_topics
            # print(curr_topic_ids-curr_positive_ids-curr_negative_ids)
            # print(f"positive ids: {len(curr_positive_ids)}")
            # print(f"negative ids: {len(curr_negative_ids)}")
            # print(f"all ids: {len(curr_topic_ids)}")
            # print(f"intersection is: {len(curr_positive_ids.intersection(curr_negative_ids))}")
            if len(curr_topic_ids) == len(curr_positive_ids)+len(curr_negative_ids):
                results = ResultCollector.get_results()
                # DocCache.add_elem(doc_hash, query_hash, results)
                SubscriberManager.reset_subscriber_count()
                DocumentCollection.add_document(
                    id, Document(id, sorted(results))
                )
                ResultCollector.reset_results()
                # print("i am here")
                return ErrorCode.EC_SUCCESS
            else:
                # print(f"len of results is: {len(results)}")
                curr_topic_ids = curr_topic_ids - curr_positive_ids - curr_negative_ids
                topics = TopicManager.get_topics_by_keys(curr_topic_ids)
                # print(f"len of topics in this case: {len(topics)}")
        else:
            topics = set(TopicManager.get_active_topics().values())

        # results = DocCache.find_elem(doc_hash, query_hash)
        topics_to_shut = set()

        for word in doc_words:
            for topic in topics:
                if(topic.receive(word)):
                    topic.matched()
                    topics_to_shut.add(topic)
            topics -= topics_to_shut
            topics_to_shut.clear()
            if topics is None:
                break

        negative_topic_ids = set((topic.id for topic in topics))

        # topics_to_reset = set(TopicManager.get_active_topics().values())
        # for topic in topics_to_reset:
        #     topic.reset_subscribers()
        # print(f"num topics at the end: {len(topics)}")
        SubscriberManager.reset_subscriber_count()
        results = ResultCollector.get_results()
        # results = get_results(doc_hash, query_hash)
        ProcessedTopicsCache.add_doc(doc_hash, curr_topic_ids-negative_topic_ids, negative_topic_ids)
        # DocCache.add_elem(doc_hash, query_hash, results)
        DocumentCollection.add_document(
            id, Document(id, sorted(results))
        )
        ResultCollector.reset_results()
        # print(f"for doc {id} cache stats are: {CalcMatch.cache_info()}")
        return ErrorCode.EC_SUCCESS

    @staticmethod
    def StartQuery(
        id: int, dist_type: int, words: list[str], tolerance: int = None
    ) -> None:
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
    def GenNextAvailableRes(res_id: int, empty_list: List):
        if DocumentCollection.get_doc_results == 0:
            return ErrorCode.EC_NO_AVAIL_RES
        results = copy.deepcopy(DocumentCollection.get_doc_results(res_id))
        assert results.id == res_id
        empty_list.extend(results.query_ids)
        DocumentCollection.remove_doc(res_id)
        return ErrorCode.EC_SUCCESS
