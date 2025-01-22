import copy
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from functools import lru_cache, cache
from typing import Set, List, Tuple
from conditional_cache import lru_cache

import Levenshtein

class DistanceType(Enum):
    NORMAL = 0
    HAMMING = 1
    THIRD = 2

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}.{self.name}"

class Document:
    def __init__(self, id, num_results, query_ids):
        self.id: int = id
        self.num_results: int = num_results
        self.query_ids: Set[int] = query_ids

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

class Subscriber(Query):
    def __init__(self, id: int, dist_type: DistanceType, keywords: list[str], tolerance: int = None):
        super().__init__(id, dist_type, keywords, tolerance)
        self.num_words_to_satisfy: int = len(self.keywords)
        # self.satisfied = False

    def word_satisfied(self):
        # make sure this is thread-safe somehow
        self.num_words_to_satisfy -= 1
        # if id==198:
        #     print(f"I have {len(self.keywords) - self.num_words_to_satisfy} left to satisfy")
        if self.num_words_to_satisfy == 0:
            # self.satisfied = True
            ResultCollector.add_result(self.id)
            self.num_words_to_satisfy = len(self.keywords)
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
        # self.status: bool[True | False | None] = None

    # find a way to turn off the topic after it has been matched
    def receive(self, doc_word):
        # if id==198:
        #     print(f"I have {len(self.subscribers)} subscribers")
        if CalcMatch(self.dist_type, self.word, doc_word, self.tolerance):
            # self.status = True
            return True
        return False
            # self.matched()

    def matched(self):
        for _, subscriber in self.subscribers.items():
            # if id==198:
            #     print("I am here")
            subscriber.word_satisfied()
        # self.status = False
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

def get_queries():
    return Subscriber.get_active_subscribers()

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

def EndQuery(id: int):
    SubscriberManager.remove_subscriber(id)

class DocCache:
    doc_results: dict[Tuple[int, int]: List[int]] = {}

    @staticmethod
    def add_elem(words_hash, queries_hash, results):
        if words_hash not in DocCache.doc_results:
            if len(DocCache.doc_results) > 256:
                del DocCache.doc_results[list(DocCache.doc_results.keys())[0]]
                # DocCache.doc_results.pop(next(iter(DocCache.doc_results))) 
            DocCache.doc_results[words_hash] = {queries_hash: results}
        else:
            if queries_hash not in DocCache.doc_results[words_hash]:
                DocCache.doc_results[words_hash] = {queries_hash: results}
    @staticmethod
    def find_elem(words_hash, queries_hash):
        if words_hash in DocCache.doc_results:
            if queries_hash in DocCache.doc_results[words_hash]:
                # print("cache")
                # print(len(DocCache.doc_results[words_hash].values()))
                return DocCache.doc_results[words_hash][queries_hash]

    # @staticmethod
    # def add_elem(words_hash, queries_hash, results):
    #     if (words_hash, queries_hash) not in DocCache.doc_results.keys():
    #         if len(DocCache.doc_results) >= 128:
    #             DocCache.doc_results.pop(next(iter(DocCache.doc_results))) 

    #         DocCache.doc_results[(words_hash, queries_hash)] = results

    # @staticmethod
    # def find_elem(words_hash, queries_hash):
    #     if (words_hash, queries_hash) in DocCache.doc_results.keys():
    #         # print("cached results")
    #         return DocCache.doc_results[(words_hash, queries_hash)]

@lru_cache(maxsize=128)
def get_results(doc_hash, queries_hash):
    return ResultCollector.get_results()

def MatchDocument(id: int, doc_words: list[str]):
    topics = set(TopicManager.get_active_topics().values())

    doc_hash = hash(doc_words)
    query_hash = hash(tuple(SubscriberManager.get_active_subscribers()))

    results = get_results(doc_hash, query_hash)

    # results = DocCache.find_elem(doc_hash, query_hash)
    if results:
        # print("cached")
        DocumentCollection.add_document(
        id, Document(id, len(results), sorted(results))
        )
        return
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

    topics_to_reset = set(TopicManager.get_active_topics().values()) - topics
    for topic in topics_to_reset:
        topic.reset_subscribers()
    # print(f"num topics at the end: {len(topics)}")
    # SubscriberManager.reset_subscriber_count()

    # results = ResultCollector.get_results()
    results = get_results(doc_hash, query_hash)
    # DocCache.add_elem(doc_hash, query_hash, results)
    DocumentCollection.add_document(
        id, Document(id, len(results), sorted(results))
    )
    ResultCollector.reset_results()
    # print(f"for doc {id} cache stats are: {CalcMatch.cache_info()}")


def GenNextAvailableRes(res_id: int):
    results = copy.deepcopy(DocumentCollection.get_doc_results(res_id))
    assert results.id == res_id
    DocumentCollection.remove_doc(res_id)
    return results.num_results, results.query_ids
