from Loader import Loader
from Extractor import APIExtractor
import psycopg2
from psycopg2 import sql
from nltk.stem import PorterStemmer
from datetime import datetime
import os
from abc import ABC, abstractmethod
import re
import logging

# Configure the logger
logging.basicConfig(filename='transformer.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

import nltk

# Add the path to the stopwords and punkt directory
stopwords_path = '/Users/haraldjentoftstuge/Downloads'
nltk.data.path.append(stopwords_path)
nltk.download('stopwords')

punkt_path = '/Users/haraldjentoftstuge/Downloads'
nltk.data.path.append(punkt_path)
nltk.download('punkt')

from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

# Create an instance of PorterStemmer
ps = PorterStemmer()


class Transformer:
    def __init__(self, data):
        self.data = data

    @abstractmethod
    def transform_news(self):
        # Override this method in subclasses to implement specific transformations
        pass

    @abstractmethod
    def transform_weather(self):
        # Override this method in subclasses to implement specific transformations
        pass

    def handle_missing_values(self, data, default_value):
        # Iterate through the data dictionary
        cleaned_data = {}
        for key, value in data.items():
            if value is None or value == "":
                cleaned_data[key] = default_value
            else:
                cleaned_data[key] = value
        return cleaned_data

    def pre_process_text(self, text):
        text = re.sub(r'\W', ' ', text)  # Remove special characters
        text = text.lower()  # Convert to lowercase
        tokens = word_tokenize(text)  # Tokenize the text
        stop_words = stopwords.words('english')  # Define the stop words
        cleaned_tokens = [ps.stem(token) for token in tokens if not token in stop_words]  # Stem the tokens
        return " ".join(cleaned_tokens)  # Return the cleaned tokens as a single string

    def convert_news_date(self, published_at):
        # Parse the publishedAt string to a datetime object, excluding the 'Z' at the end
        dt = datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%S%z")

        date_str = dt.strftime("%Y-%m-%d")
        time_str = dt.strftime("%H:%M:%S")

        return date_str, time_str


    def convert_weather_date(self, unix_time):
        # Convert the Unix timestamp to a datetime object
        dt = datetime.utcfromtimestamp(unix_time)

        date_str = dt.strftime("%Y-%m-%d")
        time_str = dt.strftime("%H:%M:%S")

        return date_str, time_str


class TextTransformer(Transformer):
    def __init__(self, data):
        super().__init__(data)

    def transform_news(self):
        # Call the base class methods as needed for text transformation
        # Handle missing values
        cleaned_data = self.handle_missing_values(self.data, default_value="N/A")

        try:
            # Pre-process text
            for i in range(cleaned_data['totalResults']):
                cleaned_data['articles'][i]['title'] = self.pre_process_text(cleaned_data['articles'][i]['title'])
                print("yayo")


                cleaned_data['articles'][i]['description'] = self.pre_process_text(cleaned_data['articles'][i]['description'])


                cleaned_data['articles'][i]['content'] = self.pre_process_text(cleaned_data['articles'][i]['content'])
        except Exception as e:
            # Log the error
            logging.error(f"Error occurred during data transformation: {str(e)}")

        # Convert time for news data
        date, time = self.convert_news_date(cleaned_data['publishedAt'])
        cleaned_data['date'] = date
        cleaned_data['time'] = time

        return cleaned_data

    def transform_weather(self):
        # Call the base class methods as needed for text transformation
        # Handle missing values
        cleaned_data = self.handle_missing_values(self.data, default_value="N/A")

        # Convert time for weather data
        if 'dt' in cleaned_data:
            date, time = self.convert_weather_date(cleaned_data['dt'])
            cleaned_data['date'] = date
            cleaned_data['time'] = time

        return cleaned_data
