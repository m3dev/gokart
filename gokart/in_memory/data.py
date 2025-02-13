from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class InMemoryData:
    value: Any
    last_modification_time: datetime

    @classmethod
    def create_data(self, value: Any) -> 'InMemoryData':
        return InMemoryData(value=value, last_modification_time=datetime.now())
