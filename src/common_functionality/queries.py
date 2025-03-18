from src.common_functionality.distances import DistanceType


class Query:
    def __init__(
        self,
        id: int,
        dist_type: DistanceType,
        keywords: list[str],
        tolerance: int = None,
    ):
        self.id: int = id
        self.dist_type: DistanceType = dist_type
        self.keywords: list[str] = keywords
        self.tolerance: int = tolerance

    def __repr__(self) -> str:
        class_name = self.__class__.__name__
        return (
            f"Class {class_name} with attributes, query_id: {self.id},"
            f"dist_type: {self.dist_type}, keywords: {self.keywords},"
            f"tolerance: {self.tolerance}"
        )
