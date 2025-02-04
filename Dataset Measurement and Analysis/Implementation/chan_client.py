# 4chan api client that has minimal functionality to collect data

import logging
import requests

# logger setup
logger = logging.getLogger("4chan client")
logger.propagate = False
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)

# API_BASE = "http://a.4cdn.org"


class ChanClient:
    API_BASE = "http://a.4cdn.org"
    # def __init__(self):

    # need to be able to collect threads
    """
    Get json for a given thread
    """

    def get_thread(self, board, thread_number):
        # sample api call: http://a.4cdn.org/pol/thread/124205675.json
        # make an http request to the url
        request_pieces = [board, "thread", f"{thread_number}.json"]

        api_call = self.build_request(request_pieces)
        return self.execute_request(api_call)

    """
    Get catalog json for a given board
    """

    def get_catalog(self, board):
        request_pieces = [board, "catalog.json"]
        api_call = self.build_request(request_pieces)

        return self.execute_request(api_call)

    """
    Build a request from pieces
    """

    def build_request(self, request_pieces):
        api_call = "/".join([self.API_BASE] + request_pieces)
        return api_call

    """
    This executes an http request and returns json
    """

    def execute_request(self, api_call):

        try:
            # Attempt to make the GET request
            resp = requests.get(api_call, timeout=10)  # Set a timeout of 10 seconds

            logger.info(resp.status_code)
            
            # Raise an HTTPError for bad responses (4xx or 5xx)
            resp.raise_for_status()

            json = resp.json()

            logger.info(f"json: {json}")
            
            # Return the JSON response if no errors occur
            return json

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")  # Log the specific HTTP error
            return None  # Return None or handle the error appropriately

        except requests.exceptions.ConnectionError as conn_err:
            print(f"Connection error occurred: {conn_err}")  # Log connection errors
            return None  # Return None or handle the error appropriately

        except requests.exceptions.Timeout as timeout_err:
            print(f"Timeout error occurred: {timeout_err}")  # Log the timeout error
            return None  # Return None or retry the request

        except requests.exceptions.RequestException as req_err:
            print(f"An error occurred: {req_err}")  # Log any other request-related errors
            return None  # Return None or handle the error appropriately

        except ValueError as json_err:
            print(f"JSON decode error: {json_err}")  # Log JSON parsing errors
            return None  # Return None if the response body isn't valid JSON

if __name__ == "__main__":
    client = ChanClient()
    json = client.get_thread("pol", 124205675)
    print(json)
