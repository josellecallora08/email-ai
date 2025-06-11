from dotenv import load_dotenv
import os
import requests
load_dotenv()

class OkpoService:
    def __init__(self):
        self.assistant_id: str = os.getenv('OKPO_ASSISTANT_ID')
        self.api_token: str = os.getenv("OKPO_API_TOKEN")
        self.base_url: str = "https://okpo.com/api/1.1/wf"
        self.create_thread_and_run_endpoint = f"{self.base_url}/create_thread_and_run"
        self.add_run_message_endpoint = f"{self.base_url}/add_run_message"
        self.retrieve_run_endpoint = f"{self.base_url}/retrieve_run"
        self.retrieve_run_message_endpoint = f"{self.base_url}/retrieve_run_message"
        self.get_assistant_endpoint = f"{self.base_url}/get_assistant"
    
    def create_thread_and_run(self, message: str):
        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json"
        }
        payload = {
            "assistant_id": self.assistant_id,
            "user_message": message
        }
        response = requests.post(self.create_thread_and_run_endpoint, json=payload, headers=headers)
        response.raise_for_status()
        return response.json()
      
    def add_run_message(self, message: str, thread_id: str):
        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json"
        }
        payload = {
            "assistant_id": self.assistant_id,
            "user_message": message,
            "thread_id": thread_id
        }
        try:
            response = requests.post(self.add_run_message_endpoint, json=payload, headers=headers)
            response.raise_for_status()
            try:
                data = response.json()
            except ValueError as json_err:
                raise Exception(f"Failed to parse JSON response: {json_err}. Response text: {response.text}")
            if not isinstance(data, dict):
                raise Exception(f"Unexpected response format: expected a dictionary, got {type(data)}. Response: {data}")
            return data
        except requests.HTTPError as http_err:
            status_code = http_err.response.status_code if http_err.response else "No status code"
            content = http_err.response.text if http_err.response else "No response content"
            raise Exception(
                f"HTTP error occurred while adding run message: {http_err}. "
                f"Status code: {status_code}. Response content: {content}"
            ) from http_err
        except requests.RequestException as req_err:
            raise Exception(f"Request error occurred while adding run message: {req_err}") from req_err
        except Exception as err:
            raise Exception(f"An unexpected error occurred while adding run message: {err}") from err
    
    def retrieve_run(self, thread_id: str, run_id: str):
        print(f"{self.retrieve_run_endpoint}/?thread_id={thread_id}&run_id={run_id}")
        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json"
        }
        response = requests.get(f"{self.retrieve_run_endpoint}/?thread_id={thread_id}&run_id={run_id}", headers=headers)
        response.raise_for_status()
        return response.json()
    
    def retrieve_run_message(self, thread_id: str, run_id: str):
 
        print(f"{self.retrieve_run_message_endpoint}")
        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json"
        }

        payload = {
            "thread_id": thread_id,
            "run_id": run_id
        }

        response = requests.post(f"{self.retrieve_run_message_endpoint}", json=payload, headers=headers)
        print(response)
        response.raise_for_status()
        response_data = response.json()

        return response_data

        
    def get_assistant(self, assistant_id: str):
        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json"
        }
        url = f"{self.get_assistant_endpoint}/?assistant_id={assistant_id}"
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            assistant_data = response.json()
            if not isinstance(assistant_data, dict):
                raise ValueError("Unexpected response format: expected a dictionary.")
            return assistant_data
        except requests.HTTPError as http_err:
            raise Exception(f"HTTP error occurred while retrieving assistant: {http_err}") from http_err
        except Exception as err:
            raise Exception(f"An error occurred while retrieving assistant: {err}") from err