from dataclasses import dataclass
from typing import Any
from abc import ABC
from datetime import datetime

class BaseData(ABC):
    ...

@dataclass
class InMemoryData(BaseData):
    value: Any
    last_modified_time: datetime

    @classmethod
    def create_data(self, value: Any) -> 'InMemoryData':
        return InMemoryData(value=value, last_modified_time=datetime.now())