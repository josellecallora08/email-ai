from lark_oapi.api.bitable.v1 import *
from app.services import LarkRecordProcessor

def main():
    # Initialize the processor
    processor = LarkRecordProcessor()
    
    print("Starting Lark Record Processor with continuous polling...")
    print("Press Ctrl+C to stop")
    
    # Option 1: Run with continuous polling (recommended)
    processor.run_with_continuous_polling(interval=5)
    
    # Option 2: Manual control (alternative approach)
    # processor.start_continuous_polling(app_token, table_id, interval=5)
    # 
    # try:
    #     # Your main application logic here
    #     while True:
    #         time.sleep(1)
    # except KeyboardInterrupt:
    #     processor.stop_continuous_polling()
    
    # Option 3: One-time processing (for testing)
    # success = processor.fetch_unprocessed_records(app_token, table_id)
    # if success:
    #     processor.process_all_records(app_token, table_id)
    # else:
    #     print("Failed to fetch records")

if __name__ == "__main__":
    main()