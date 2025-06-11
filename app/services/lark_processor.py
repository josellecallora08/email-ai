import json
import lark_oapi as lark
from lark_oapi.api.bitable.v1 import *
import time
from datetime import datetime, timedelta
from queue import Queue
import threading
import signal
import sys
from app.handlers import EmailSendingHandler
from dotenv import load_dotenv
import os

load_dotenv()

class LarkRecordProcessor:
    def __init__(self, log_level=lark.LogLevel.DEBUG):
        """
        Initialize the Lark Record Processor
        
        Args:
            log_level: Logging level for the client
        """

        self.record_queue = Queue()
        self.is_running = False
        self.polling_thread = None
        self.is_processing = False
        self.processing_lock = threading.Lock()
        self.app_token = os.getenv("LARK_BITABLE_ID")
        self.table_id = os.getenv("LARK_TABLE_ID")
        self.app_id = os.getenv("LARK_APP_ID")
        self.app_secret = os.getenv("LARK_APP_SECRET")

        self.client = lark.Client.builder() \
            .app_id(self.app_id) \
            .app_secret(self.app_secret) \
            .log_level(log_level) \
            .build()
        
    def fetch_records(self, filter_condition: str, page_size: int = 20) -> bool:
        """
        Fetch records from Lark BiTable and add them to the queue
        
        Args:
            filter_condition: Filter condition for the records
            page_size: Number of records per page
            
        Returns:
            bool: True if successful, False otherwise
        """
        request: ListAppTableRecordRequest = ListAppTableRecordRequest.builder() \
            .app_token(self.app_token) \
            .table_id(self.table_id) \
            .filter(filter_condition) \
            .page_size(page_size) \
            .build()

        response: ListAppTableRecordResponse = self.client.bitable.v1.app_table_record.list(request)

        if not response.success():
            lark.logger.error(
                f"client.bitable.v1.app_table_record.list failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}, resp: \n{json.dumps(json.loads(response.raw.content), indent=4, ensure_ascii=False)}")
            return False

        lark.logger.info(lark.JSON.marshal(response.data, indent=4))
        
        # Add all retrieved records to the queue
        if response.data and response.data.items:
            for record in response.data.items:
                self.record_queue.put(record)
            
            print(f"Added {len(response.data.items)} records to the queue")
            print(f"Queue size: {self.record_queue.qsize()}")
            return True
        
        print("No records found or retrieved")
        return False
    
    def process_record(self, record) -> bool:
        """
        Process a single record and update its status to "Processed"
        
        Args:
            record: The record to process
            
        Returns:
            bool: True if successful, False otherwise
        """
        print(f"Processing record ID: {record.record_id}")
        print(f"Processing records: {record.fields}")
        
        # Add your custom processing logic here
        # This is a placeholder implementation
        
        # Update the processed_status field to "Processed"
        return self.update_record_status(record.record_id, "Processed")
    
    def process_all_records(self) -> None:
        """
        Process all records in the queue and update their status
        """
        with self.processing_lock:
            self.is_processing = True
            try:
                processed_count = 0
                failed_count = 0
                total_records = self.get_queue_size()
                
                print(f"Starting to process {total_records} records...")
                
                while not self.record_queue.empty():
                    record = self.record_queue.get()
                    email = EmailSendingHandler()                    
                    try:
                        success = email.handler(payload=record)
                        if success:
                            processed_count += 1
                        else:
                            failed_count += 1
                        self.record_queue.task_done()
                        
                        # Show progress
                        current_progress = processed_count + failed_count
                        print(f"Progress: {current_progress}/{total_records} records processed")
                        
                    except Exception as e:
                        lark.logger.error(f"Error processing record {record.record_id}: {str(e)}")
                        failed_count += 1
                        self.record_queue.task_done()
                
                print(f"Processing complete: {processed_count} successful, {failed_count} failed")
            finally:
                self.is_processing = False
    
    def get_queue_size(self) -> int:
        """
        Get the current size of the queue
        
        Returns:
            int: Number of items in the queue
        """
        return self.record_queue.qsize()
    
    def is_queue_empty(self) -> bool:
        """
        Check if the queue is empty
        
        Returns:
            bool: True if queue is empty, False otherwise
        """
        return self.record_queue.empty()
    
    def fetch_unprocessed_records(self, page_size: int = 20) -> bool:
        """
        Fetch only unprocessed records from Lark BiTable
        
        Args:
            page_size: Number of records per page
            
        Returns:
            bool: True if successful, False otherwise
        """
        # Updated filter to exclude already processed records
        filter_condition = 'AND(CurrentValue.[Label].contains("Willing to Pay", "Willing To Pay"), NOT(CurrentValue.[processed_status]="Processed"))'
        
        return self.fetch_records(filter_condition, page_size)
    
    def start_continuous_polling(self, interval: int = 5) -> None:
        """
        Start continuous polling for new unprocessed records
        
        Args:
            interval: Polling interval in seconds (default: 5)
        """
        if self.is_running:
            print("Polling is already running")
            return
        
        self.is_running = True
        self.polling_thread = threading.Thread(
            target=self._polling_loop,
            args=(interval,),
            daemon=True
        )
        self.polling_thread.start()
        print(f"Started continuous polling every {interval} seconds")
    
    def stop_continuous_polling(self) -> None:
        """
        Stop the continuous polling
        """
        if self.is_running:
            self.is_running = False
            if self.polling_thread:
                self.polling_thread.join(timeout=10)
            print("Stopped continuous polling")
        else:
            print("Polling is not running")
    
    def _polling_loop(self, interval: int) -> None:
        """
        Internal polling loop that runs in a separate thread
        
        Args:
            interval: Polling interval in seconds
        """
        print("Polling loop started")
        
        while self.is_running:
            try:
                # Check if currently processing queue
                if self.is_processing:
                    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    print(f"[{current_time}] Queue is being processed, waiting...")
                    
                    # Wait for processing to complete
                    while self.is_processing and self.is_running:
                        time.sleep(1)
                    
                    if not self.is_running:
                        break
                    
                    print("Queue processing completed, resuming polling...")
                
                # Only check for new records if not processing and queue is empty
                if not self.is_processing and self.is_queue_empty():
                    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    print(f"\n[{current_time}] Checking for new unprocessed records...")
                    
                    # Fetch unprocessed records
                    success = self.fetch_unprocessed_records()
                    
                    if success and not self.is_queue_empty():
                        print(f"Found {self.get_queue_size()} new records to process")
                        
                        # Process all records in the queue
                        self.process_all_records()
                    else:
                        print("No new records found")
                elif not self.is_processing and not self.is_queue_empty():
                    # Queue has items but not processing (shouldn't happen, but safety check)
                    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    print(f"[{current_time}] Queue has {self.get_queue_size()} items, processing...")
                    self.process_all_records()
                
                # Wait for the specified interval
                for _ in range(interval):
                    if not self.is_running:
                        break
                    time.sleep(1)
                    
            except Exception as e:
                lark.logger.error(f"Error in polling loop: {str(e)}")
                time.sleep(interval)  # Wait before retrying
        
        print("Polling loop stopped")
    
    def run_with_continuous_polling(self, interval: int = 5) -> None:
        """
        Run the processor with continuous polling and handle graceful shutdown
        
        Args:
            interval: Polling interval in seconds (default: 5)
        """
        def signal_handler(sig, frame):
            print("\nReceived interrupt signal. Shutting down gracefully...")
            self.stop_continuous_polling()
            sys.exit(0)
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Start continuous polling
        self.start_continuous_polling(interval)
        
        try:
            # Keep the main thread alive
            while self.is_running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop_continuous_polling()
    
    def clear_queue(self) -> None:
        """
        Clear all items from the queue
        """
        while not self.record_queue.empty():
            try:
                self.record_queue.get_nowait()
                self.record_queue.task_done()
            except:
                break
        print("Queue cleared")
    
    def get_processing_status(self) -> dict:
        """
        Get the current processing status
        
        Returns:
            dict: Current status information
        """
        return {
            "is_running": self.is_running,
            "is_processing": self.is_processing,
            "queue_size": self.get_queue_size(),
            "queue_empty": self.is_queue_empty()
        }