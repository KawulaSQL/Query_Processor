import sys
sys.path.append('./Concurrency_Control_Manager')

from Concurrency_Control_Manager.ConcurrencyControlManager import ConcurrencyControlManager
from Concurrency_Control_Manager.models.Operation import Operation
from Concurrency_Control_Manager.models.CCManagerEnums import OperationType, ResponseType
from Failure_Recovery import FailureRecoveryManager
from utils.models import ExecutionResult, Rows
from datetime import datetime

class QueryConcurrencyController:
    operations: list[ExecutionResult]

    def __init__(self):
        self.ccm = ConcurrencyControlManager()
        self.frm = FailureRecoveryManager.FailureRecoveryManager("Failure_Recovery/wal.log")
        self.is_transacting = False
        self.operations = []
        self.queries_operations = []
        self.failed_operations = []
        self.transact_id = 1
        self.is_rollingback = False
    
    def begin_transaction(self):
        self.is_transacting = True
    
    def check_for_response_select(self, table_names: list[str]) -> list | str:
        self.transact_id = self.ccm.begin_transaction()
        try:
            for res_string in table_names:
                ops = Operation(self.transact_id, OperationType.R, f"{res_string}")
                response = self.ccm.validate_object(ops)
                if response.responseType.name == "ALLOWED":
                    self.ccm.log_object(ops)
                elif response.responseType.name == "ABORT":
                    rollback = self.frm.recover(self.transact_id)
                    self.is_rollingback = True
                    self.failed_operations.append(ops)
                    self.ccm.end_transaction(self.transact_id)
                    return rollback
                else:
                    return "LE WAIT"
            self.ccm.end_transaction(self.transact_id)
            return "OK"
        except Exception as e:
            print(f"BLa bla bla: {e}")
    
    def check_for_response_insert(self, table_names: list[str]) -> list | str:
        self.transact_id = self.ccm.begin_transaction()
        for res_string in table_names:
            ops = Operation(self.transact_id, OperationType.W, f"{res_string}")
            response = self.ccm.validate_object(ops)
            if response.responseType.name == "ALLOWED":
                self.ccm.log_object(ops)
            elif response.responseType.name == "ABORT":
                rollback = self.frm.recover(self.transact_id)
                self.is_rollingback = True
                self.failed_operations.append(ops)
                self.ccm.end_transaction(self.transact_id)
                return rollback
        self.ccm.end_transaction(self.transact_id)
        return "OK"
    
    def check_for_response_update(self, table_names: list[str]) -> list | str:
        self.transact_id = self.ccm.begin_transaction()
        for res_string in table_names:
            ops = Operation(self.transact_id, OperationType.R, f"{res_string}")
            response = self.ccm.validate_object(ops)
            if response.responseType.name == "ALLOWED":
                self.ccm.log_object(ops)
            elif response.responseType.name == "ABORT":
                rollback = self.frm.recover(self.transact_id)
                self.is_rollingback = True
                self.failed_operations.append(ops)
                self.ccm.end_transaction(self.transact_id)
                return rollback
        for res_string in table_names:
            ops = Operation(self.transact_id, OperationType.W, f"{res_string}")
            response = self.ccm.validate_object(ops)
            if response.responseType.name == "ALLOWED":
                self.ccm.log_object(ops)
            elif response.responseType.name == "ABORT":
                rollback = self.frm.recover(self.transact_id)
                self.is_rollingback = True
                self.failed_operations.append(ops)
                self.ccm.end_transaction(self.transact_id)
                return rollback
        return "OK"
    
    def check_for_response_delete(self, table_names: list[str]) -> list | str:
        self.transact_id = self.ccm.begin_transaction()
        for res_string in table_names:
            ops = Operation(self.transact_id, OperationType.R, f"{res_string}")
            response = self.ccm.validate_object(ops)
            if response.responseType.name == "ALLOWED":
                self.ccm.log_object(ops)
            elif response.responseType.name == "ABORT":
                rollback = self.frm.recover(self.transact_id)
                self.is_rollingback = True
                self.failed_operations.append(ops)
                self.ccm.end_transaction(self.transact_id)
                return rollback
        for res_string in table_names:
            ops = Operation(self.transact_id, OperationType.W, f"{res_string}")
            response = self.ccm.validate_object(ops)
            if response.responseType.name == "ALLOWED":
                self.ccm.log_object(ops)
            elif response.responseType.name == "ABORT":
                rollback = self.frm.recover(self.transact_id)
                self.is_rollingback = True
                self.failed_operations.append(ops)
                self.ccm.end_transaction(self.transact_id)
                return rollback
        return "OK"
    
    def end_transaction(self):
        res = ExecutionResult(
            transaction_id=self.transact_id,
            timestamp=datetime.now(),
            type="COMMIT",
            status="success",
            query="COMMIT",
            previous_data=Rows(data=[], rows_count=0, schema=[], columns=[]),
            new_data=Rows(data=[], rows_count=0, schema=[], columns=[])
        )
        self.frm.write_log(res)
        self.is_transacting = False
        self.ccm.end_transaction(self.transact_id)