from datetime import datetime
from typing import Generic, List, TypeVar, Union, Tuple
from dataclasses import dataclass

T = TypeVar('T')

@dataclass
class Rows(Generic[T]):
    data: List[T]
    rows_count: int
    columns: List[str]

@dataclass
class ExecutionResult:
    transaction_id: int
    timestamp: datetime
    status: str
    query: str
    previous_data: Union[Rows, int]
    new_data: Union[Rows, int]