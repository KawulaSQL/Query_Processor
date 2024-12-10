from datetime import datetime
from typing import Generic, List, TypeVar, Union, Dict, Optional
from dataclasses import dataclass

T = TypeVar('T')

@dataclass
class Rows(Generic[T]):
    data: List[T]
    rows_count: int
    schema: Optional[List[str]] = None
    columns: Optional[Dict[str, str]] = None

@dataclass
class ExecutionResult:
    transaction_id: int
    timestamp: datetime
    type: str
    status: str
    query: str
    previous_data: Union[Rows, int, None]
    new_data: Union[Rows, int, None]