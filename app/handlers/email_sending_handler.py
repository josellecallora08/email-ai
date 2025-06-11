from app.services import LarkBaseRecords
from typing import Dict, Any

class EmailSendingHandler:
    def __init__(self) -> None:
        self.lark_processor = LarkBaseRecords()

    def handler(self, payload: Any):
        """
        Handle the record processing
        
        Args:
            payload: The record object from Lark (AppTableRecord instance)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # The payload is an AppTableRecord object, so we access record_id directly
            print(f"Processing record: {payload.record_id}")
            print(f"Record fields: {payload.fields}")
            
            # Add your email sending logic here
            # For now, we'll just update the status
            
            return self.lark_processor.update_record_status(payload.record_id, status="Processed")
        except Exception as e:
            print(f"Error in email handler: {str(e)}")
            return False