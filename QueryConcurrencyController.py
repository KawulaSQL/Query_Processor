import sys
sys.path.append('./Concurrency_Control_Manager')

from Concurrency_Control_Manager.ConcurrencyControlManager import ConcurrencyControlManager
from Concurrency_Control_Manager.models.Operation import Operation
from Concurrency_Control_Manager.models.CCManagerEnums import OperationType, ResponseType
from Failure_Recovery import FailureRecoveryManager
from utils.models import ExecutionResult, Rows

class QueryConcurrencyController:
    def __init__(self):
        self.ccm = ConcurrencyControlManager()
        self.frm = FailureRecoveryManager.FailureRecoveryManager("path")
        self.is_transacting = False
        self.operations = []
        self.transact_id = 1
        self.is_rollingback = False
    
    def begin_transaction(self):
        self.is_transacting = True
        self.transact_id = self.ccm.begin_transaction()
    
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
            elif response.responseType == "ABORT":
                rollback = self.frm.recover(self.transact_id)
                self.is_rollingback = True
                return rollback
        for res_string in table_names:
            ops = Operation(self.transact_id, OperationType.W, f"{res_string}")
            response = self.ccm.validate_object(ops)
            if response.responseType == "Allowed":
                self.ccm.log_object(ops)
            elif response.responseType == "Abort":
                rollback = self.frm.recover(self.transact_id)
                self.is_rollingback = True
                return rollback
        self.ccm.end_transaction(self.transact_id)
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
                return rollback
        for res_string in table_names:
            ops = Operation(self.transact_id, OperationType.W, f"{res_string}")
            response = self.ccm.validate_object(ops)
            if response.responseType.name == "ALLOWED":
                self.ccm.log_object(ops)
            elif response.responseType.name == "ABORT":
                rollback = self.frm.recover(self.transact_id)
                self.is_rollingback = True
                return rollback
        self.ccm.end_transaction(self.transact_id)
        return "OK"
    
    def end_transaction(self):
        self.ccm.end_transaction(self.transact_id)
        self.is_transacting = False