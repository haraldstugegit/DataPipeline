import requests
from abc import ABC, abstractmethod
import logging

# Configure the logger
logging.basicConfig(filename='extractor.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

class Extractor(ABC):
    @abstractmethod
    def extract(self):
        pass

class APIExtractor(Extractor):
    def __init__(self, url):
        self.url = url

    def extract(self):
        try:
            response = requests.get(api_url)
            if response.status_code == 200:

                    data = response.json()
                    return data
            else:
                    # Handle API error
                    print(f"API request failed with status code: {response.status_code}")

        except requests.exceptions.RequestException as e:
                # Handle network/connection error
                print(f"API request failed: {str(e)}")
                logging.error(f"An error occurred: {str(e)}")


        #Incorporate handling of other Exceptions
        except Exception as e:

                print(f"An error occurred: {str(e)}")
