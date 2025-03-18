from enum import Enum


class ErrorCode(Enum):
    EC_SUCCESS = 0
    EC_NO_AVAIL_RES = 1
    EC_FAIL = 2

    def __str__(self):
        return f"{self.name} (Code {self.value})"
