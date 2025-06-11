import lark_oapi as lark
from lark_oapi.api.bitable.v1 import *
from dotenv import load_dotenv
import os

load_dotenv()

class LarkBaseRecords: 
    def __init__(self) -> None:
        self.app_token = os.getenv("LARK_BITABLE_ID")
        self.table_id = os.getenv("LARK_TABLE_ID")
        self.app_id = os.getenv("LARK_APP_ID")
        self.app_secret = os.getenv("LARK_APP_SECRET")
        
        # Initialize the client - this was missing!
        self.client = lark.Client.builder() \
            .app_id(self.app_id) \
            .app_secret(self.app_secret) \
            .log_level(lark.LogLevel.DEBUG) \
            .build()
    
    def update_record_status(self, record_id: str, status: str) -> bool:
        """
        Update the processed_status field of a record
        
        Args:
            record_id: ID of the record to update
            status: Status to set (e.g., "Processed")
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Create the update request
            request = UpdateAppTableRecordRequest.builder() \
                .app_token(self.app_token) \
                .table_id(self.table_id) \
                .record_id(record_id) \
                .request_body(AppTableRecord.builder()
                    .fields({
                        "processed_status": status
                    })
                    .build()) \
                .build()
            
            # Execute the update
            response = self.client.bitable.v1.app_table_record.update(request)
            
            if response.success():
                print(f"Successfully updated record {record_id} status to '{status}'")
                return True
            else:
                lark.logger.error(f"Failed to update record {record_id}: {response.msg}")
                return False
                
        except Exception as e:
            lark.logger.error(f"Exception updating record {record_id}: {str(e)}")
            return False
        
    def update_single_field(self, record_id: str, field_name: str, field_value: Any) -> bool:
        """
        Update a single field of a record
        
        Args:
            record_id: ID of the record to update
            field_name: Name of the field to update
            field_value: New value for the field
            
        Returns:
            bool: True if successful, False otherwise
        """
        return self.update_record_fields(record_id, {field_name: field_value})

    def update_record_fields(self, record_id: str, fields: Dict[str, Any]) -> bool:
        """
        Update multiple fields of a record dynamically
        
        Args:
            record_id: ID of the record to update
            fields: Dictionary of field names and their new values
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Create the update request
            request = UpdateAppTableRecordRequest.builder() \
                .app_token(self.app_token) \
                .table_id(self.table_id) \
                .record_id(record_id) \
                .request_body(AppTableRecord.builder()
                    .fields(fields)
                    .build()) \
                .build()
            
            # Execute the update
            response = self.client.bitable.v1.app_table_record.update(request)
            
            if response.success():
                print(f"Successfully updated record {record_id} with fields: {fields}")
                return True
            else:
                lark.logger.error(f"Failed to update record {record_id}: {response.msg}")
                return False
                
        except Exception as e:
            lark.logger.error(f"Exception updating record {record_id}: {str(e)}")
            return False