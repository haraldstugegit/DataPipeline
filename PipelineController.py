from Extractor import APIExtractor
from Transformer import TextTransformer
from Loader import Loader
import psycopg2
from psycopg2 import sql
import logging
from flask import Flask, request, jsonify
from sqlalchemy import create_engine, text
from flask_sqlalchemy import SQLAlchemy


# Initialize the Flask application
app = Flask(__name__)

# Configure database connection
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://username:password@localhost:5432/db_name'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Initialize SQLAlchemy
db = SQLAlchemy(app)

# Configure the logger
logging.basicConfig(filename='pipeline.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')


class PipelineController:
    def __init__(self, db_name, username, password, host, port):
        # Initialize database connection parameters
        self.db_name = db_name
        self.username = username
        self.password = password
        self.host = host
        self.port = port

        # Initialize the loader
        self.loader = Loader(self.host, self.db_name, self.username, self.password)



    #@app.route('/api/weather', methods=['GET'])
    #def get_weather_data():

        #sort_by = request.args.get('sort_by', default='date', type=str)
        #location = request.args.get('location', type=str)
        # Build the SQL query
        #query = f"SELECT * FROM weather"
        #if location:
            #query += f" WHERE location = '{location}'"
        #query += f" ORDER BY {sort_by}"
        # Execute the query and return the results as JSON
        #result = db.engine.execute(text(query))
        #return jsonify([dict(row) for row in result])


    #@app.route('/api/news', methods=['GET'])
    #def get_news_data():
        # Get sorting and filtering parameters from the request
        #sort_by = request.args.get('sort_by', default='date', type=str)
        #category = request.args.get('category', type=str)
        # Build the SQL query
        #query = f"SELECT * FROM news"
        #if category:
            #query += f" WHERE category = '{category}'"
        #query += f" ORDER BY {sort_by}"
        # Execute the query and return the results as JSON
        #result = db.engine.execute(text(query))
        #return jsonify([dict(row) for row in result])

    def run_pipeline(self, url, transformer_type):
        # Step 2: Extract data from API
        try:
            extractor = APIExtractor(url)
            raw_data = extractor.extract()
            transformer = TextTransformer(raw_data)


            # Step 3: Transform raw data
            if transformer_type == 'weather':
                transformed_data = transformer.transform_weather()
                print(transformed_data)
                self.loader.load_location(transformed_data)
                self.loader.load_weather(transformed_data)

            elif transformer_type == 'news':
                transformed_data = transformer.transform_news()
                articleAmount = transformed_data['totalResults']

                for i in range(articleAmount):
                    author_id = self.loader.load_author(transformed_data['articles'][i])
                    self.loader.load_article(transformed_data['articles'][i], author_id)
        except Exception as e:
            # Log the error
            logging.error(f"Error occurred when running the pipeline loading: {str(e)}")

        else:
            print('Invalid transformer type')
            return

        print('Pipeline execution successful!')

if __name__ == "__main__":
    # Initialize the controller
    controller = PipelineController("haraldsdb", "postgres", "Solhatt59", "localhost", "5432")
    #news_url = 'https://newsapi.org/v2/top-headlines?sources=techcrunch&apiKey=d3a9d92669c3433aa43e6aae454420a0'
    #weather_url = 'https://api.openweathermap.org/data/2.5/weather?q=Trondheim,NOR&appid=276808989ed292e656b49891cede6cbb'
    #controller.run_pipeline(weather_url, 'weather')

    while True:
        print("Please select the API you want to use:")
        print("1. Weather")
        print("2. News")
        print("3. Quit")
        user_choice = input()

        if user_choice == "1":
            city = input("Enter the city: ")
            country_code = input("Enter the country code: ")
            weather_url = f"https://api.openweathermap.org/data/2.5/weather?q={city},{country_code}&appid=276808989ed292e656b49891cede6cbb"
            controller.run_pipeline(weather_url, 'weather')

        elif user_choice == "2":
            word = input("Enter a word to search for in the news: ")
            news_url = f"https://newsapi.org/v2/everything?q={word}&apiKey=d3a9d92669c3433aa43e6aae454420a0"
            controller.run_pipeline(news_url, 'news')

        elif user_choice == "3":
            print("Quitting the program...")
            break
        else:
            print("Invalid input. Please enter 1, 2 or 3.")
