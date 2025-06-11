from app.services import LarkBaseRecords, OkpoService
from typing import Dict, Any
import time
from datetime import datetime
from dotenv import load_dotenv
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Content, Attachment, FileContent, FileName, FileType, Disposition
import base64
import os
load_dotenv()
class EmailSendingHandler:
    def __init__(self) -> None:
        self.LARK_PROCESSOR = LarkBaseRecords()
        self.OKPO_PROCESSOR = OkpoService()
        self.sendgrid_api_key = os.getenv("SENDGRID_API_KEY")
        self.sender_email = os.getenv("SENDGRID_SENDER_EMAIL")
        self.sender_name = os.getenv("SENDGRID_SENDER_NAME", "Your Company")
        
        if not self.sendgrid_api_key:
            raise ValueError("SENDGRID_API_KEY environment variable is required")
        if not self.sender_email:
            raise ValueError("SENDGRID_SENDER_EMAIL environment variable is required")
        
        self.sg = SendGridAPIClient(api_key=self.sendgrid_api_key)

    def handler(self, payload: Any) -> bool:
        """
        Handle the record processing

        Args:
            payload: The record object from Lark (AppTableRecord instance)

        Returns:
            bool: True if successful, False otherwise
        """
        try:

            record = payload.fields  # ✅ CORRECT access

            # Core email components
            recipient_email = record.get("receipient")
            sender_email = record.get("sender")
            email_subject = record.get("subject")
            email_body = record.get("body")

            # Agent info
            agent_info = record.get("Agent Lark", [{}])[0]
            agent_name = agent_info.get("name", "Unknown Agent")
            thread_id = record.get("thread_id")  # could be None
            # Timestamps
            timestamp_created = record.get("Date_created")
            date_created = datetime.fromtimestamp(timestamp_created / 1000) if timestamp_created else None

            if thread_id :
                response = self.OKPO_PROCESSOR.add_run_message(message=email_body, thread_id=thread_id)
                run_id = response['response'].get('run_id')
                if not run_id:
                    raise Exception("Failed to retrieve run id after adding run message.")
            else:
                response = self.OKPO_PROCESSOR.create_thread_and_run(message=email_body)
                thread_id = response['response'].get('thread_id')
                run_id = response['response'].get('run_id')

                self.LARK_PROCESSOR.update_single_field(payload.record_id, field_name="thread_id",field_value=thread_id)

                if not thread_id or not run_id:
                    raise Exception("Failed to create thread and run.")
            
            max_retries = 30
            delay_seconds = 2
            for attempt in range(max_retries):
                run_status_response = self.OKPO_PROCESSOR.retrieve_run(thread_id=thread_id, run_id=run_id)
                print(f"retrieve_run response:", run_status_response)
                status = run_status_response['response'].get('status')
                if status == "completed":
                    break
                elif status in ("failed", "cancelled"):
                    raise Exception(f"Run ended with status: {status}")
                time.sleep(delay_seconds)
            else:
                raise Exception("Run did not complete in expected time.")
            
            okpo_response = self.OKPO_PROCESSOR.retrieve_run_message(thread_id=thread_id, run_id=run_id)

            self.LARK_PROCESSOR.update_single_field(payload.record_id, field_name="okpo_response",field_value=okpo_response['response']['message'])


            message = Mail(
                self.sender_email,recipient_email,email_subject,okpo_response['response']['message']
            )

            self.sg.send(message)
            # Update record status
            return self.LARK_PROCESSOR.update_record_status(payload.record_id, status="Processed")

        except Exception as e:
            print(f"Error in email handler: {str(e)}")
            return False