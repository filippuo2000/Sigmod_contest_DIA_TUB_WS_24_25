class Document:
    def __init__(self, id: int, query_ids: list[int]):
        self.id: int = id
        self.query_ids: list[int] = query_ids


class DocumentCollection:
    docs: dict[int, Document] = {}

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
