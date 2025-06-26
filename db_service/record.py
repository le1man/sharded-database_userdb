from typing import NamedTuple, Dict


class Record(NamedTuple):
    fields: Dict[str, str]
    timestamp: int
