from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol


class BaseData(Protocol): ...


@dataclass
class InMemoryData(BaseData):
    value: Any
    last_modified_time: datetime

    @classmethod
    def create_data(self, value: Any) -> 'InMemoryData':
        return InMemoryData(value=value, last_modified_time=datetime.now())
