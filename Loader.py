import psycopg2
from psycopg2 import sql
import logging

# Configure the logger
logging.basicConfig(filename='loader.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

class Loader:

    def __init__(self, host, dbname, user, password):
        self.conn = psycopg2.connect(
            host=host,
            dbname=dbname,
            user=user,
            password=password
        )
        self.cur = self.conn.cursor()

    def load_location(self, transformed_data):
        try:
            insert_query = sql.SQL("""
                INSERT INTO location (city_id, city_name, country_code, lat, lon)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (city_id) DO NOTHING;
            """)
            data = [transformed_data['id'], transformed_data['name'], transformed_data['sys']['country'], transformed_data['coord']['lon'], transformed_data['coord']['lat']]
            self.cur.execute(insert_query, data)
            self.conn.commit()
        except Exception as e:
            # Log the error
            logging.error(f"Error occurred during data loading: {str(e)}")

    def load_weather(self, transformed_data):
        insert_query = sql.SQL("""
            INSERT INTO weather (weather_id, city_id, date, weather_main, weather_description, temp, temp_min, temp_max, pressure, humidity, wind_speed, clouds_all, wind_deg)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """)
        data = [transformed_data['weather'][0]['id'], transformed_data['id'], transformed_data['date'], transformed_data['time'], transformed_data['weather'][0]['main'], transformed_data['main']['temp'], transformed_data['main']['temp_min'], transformed_data['main']['temp_max'], transformed_data['main']['pressure'], transformed_data['main']['humidity'], transformed_data['wind']['speed'], transformed_data['clouds']['all'], transformed_data['wind']['deg']]
        self.cur.execute(insert_query, data)
        self.conn.commit()


    def load_author(self, transformed_data):
        insert_query = sql.SQL("""
        INSERT INTO authors (source_id, author_name)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
        RETURNING author_id;
    """)
        data = [transformed_data['source']['id'], transformed_data['author']]
        self.cur.execute(insert_query, data)

        # Try to fetch the returned id
        result = self.cur.fetchone()

        # If the result is None, the author already exists, so fetch their id
        if result is None:
            select_query = sql.SQL("""
                SELECT author_id FROM authors WHERE source_id = %s AND author_name = %s;
            """)
            self.cur.execute(select_query, data)
            result = self.cur.fetchone()

        author_id = result[0]
        self.conn.commit()
        return author_id

    def load_article(self, transformed_data, author_id):
        insert_query = sql.SQL("""
            INSERT INTO article (author_id, title, description, url, published_at, content)
            VALUES (%s, %s, %s, %s, %s, %s);
        """)
        data = [author_id, transformed_data['title'], transformed_data['description'], transformed_data['url'], transformed_data['publishedAt'], transformed_data['content']]
        self.cur.execute(insert_query, data)

        #Included a the making of a secound table which doesnt include description or content
        #This is because of some of the functionality for the pre_processing of text has some bugs
        insert_query2 = sql.SQL("""
            INSERT INTO article2 (source_id, source_name, author_id, title, url, published_at)
            VALUES (%s, %s, %s, %s, %s, %s);
        """)
        data2 = [transformed_data['source']['id'], transformed_data['source']['name'], author_id,
                transformed_data['title'], transformed_data['url'], transformed_data['publishedAt']]

        self.cur.execute(insert_query2, data2)

        self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()
