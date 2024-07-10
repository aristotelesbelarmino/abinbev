import os
import pyarrow.parquet as pq
import pandas as pd
import psycopg2
import traceback
from datetime import datetime

class GoldLayerInserter:
    def __init__(self):
        self.conn = psycopg2.connect(host='postgres', dbname='airflow', user='airflow', password='airflow')
        self.cursor = self.conn.cursor()

    def __del__(self):
        if self.conn is not None:
            self.conn.close()

    def insert_gold_layer(self, **kwargs):
        try:
            latest_date = self._find_latest_date()
            file_path = f'/usr/local/airflow/data/silver/parquet_data/{latest_date}'
            df = self._read_parquet(file_path)

            self._insert_states(df)
            self._insert_addresses(df)
            self._insert_postal_codes(df)
            self._insert_cities(df)
            self._insert_longitudes(df)
            self._insert_latitudes(df)
            self._insert_phones(df)
            self._insert_countries(df)
            self._insert_breweries(df)

            self.conn.commit()
            print("Data successfully inserted into Gold layer!")

        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Error inserting data into Gold layer:\n{traceback.format_exc()}")

    def _find_latest_date(self):
        dates = os.listdir('/usr/local/airflow/data/silver/parquet_data/')
        latest_date = sorted(dates, reverse=True)[0]
        return latest_date

    def _read_parquet(self, file_path):
        all_files = []
        for root, dirs, files in os.walk(file_path):
            for file in files:
                if file.endswith('.parquet'):
                    all_files.append(os.path.join(root, file))

        if not all_files:
            raise FileNotFoundError(f"No Parquet files found in directory: {file_path}")

        df_list = []
        for file in all_files:
            table = pq.read_table(file, use_pandas_metadata=True)
            df_list.append(table.to_pandas())

        combined_df = pd.concat(df_list, ignore_index=True)
        
        # Rename columns to ensure consistency
        column_mapping = {
            'state': 'state', 'address_1': 'address_1', 'address_2': 'address_2', 'address_3': 'address_3', 
            'postal_code': 'postal_code', 'city': 'city', 'longitude': 'longitude', 'latitude': 'latitude', 
            'phone': 'phone', 'country': 'country', 'id': 'id', 'name': 'name', 'website_url': 'website_url'
        }
        
        combined_df = combined_df.rename(columns=column_mapping)
        return combined_df

    def _get_or_create_id(self, select_query, insert_query, value):
        self.cursor.execute(select_query, (value,))
        result = self.cursor.fetchone()
        if result:
            return result[0]
        else:
            self.cursor.execute(insert_query, (value,))
            self.cursor.execute(select_query, (value,))
            return self.cursor.fetchone()[0]

    def _insert_states(self, df):
        for state in df['state'].dropna().unique():
            select_query = "SELECT state_id FROM gold_states WHERE state_name = %s"
            insert_query = "INSERT INTO gold_states (state_name) VALUES (%s)"
            self._get_or_create_id(select_query, insert_query, state)

    def _insert_addresses(self, df):
        unique_addresses = df[['address_1', 'address_2', 'address_3']].drop_duplicates().dropna()
        for _, address in unique_addresses.iterrows():
            full_address = f"{address['address_1']} {address['address_2'] or ''} {address['address_3'] or ''}".strip()
            select_query = """
                SELECT address_id FROM gold_address WHERE address_1 = %s AND address_2 = %s AND address_3 = %s
            """
            insert_query = """
                INSERT INTO gold_address (address_1, address_2, address_3, full_address)
                VALUES (%s, %s, %s, %s)
            """
            self._get_or_create_id(select_query, insert_query, (address['address_1'], address['address_2'], address['address_3'], full_address))

    def _insert_postal_codes(self, df):
        for postal_code in df['postal_code'].dropna().unique():
            select_query = "SELECT postal_code_id FROM gold_postal_code WHERE postal_code = %s"
            insert_query = "INSERT INTO gold_postal_code (postal_code) VALUES (%s)"
            self._get_or_create_id(select_query, insert_query, postal_code)

    def _insert_cities(self, df):
        unique_cities = df[['city', 'state']].dropna().drop_duplicates()
        for _, city in unique_cities.iterrows():
            state_id_query = "SELECT state_id FROM gold_states WHERE state_name = %s"
            self.cursor.execute(state_id_query, (city['state'],))
            state_id = self.cursor.fetchone()[0]

            select_query = "SELECT city_id FROM gold_cities WHERE city_name = %s AND state_id = %s"
            insert_query = "INSERT INTO gold_cities (city_name, state_id) VALUES (%s, %s)"
            self._get_or_create_id(select_query, insert_query, (city['city'], state_id))

    def _insert_longitudes(self, df):
        for longitude in df['longitude'].dropna().unique():
            select_query = "SELECT longitude_id FROM gold_longitude WHERE longitude = %s"
            insert_query = "INSERT INTO gold_longitude (longitude) VALUES (%s)"
            self._get_or_create_id(select_query, insert_query, longitude)

    def _insert_latitudes(self, df):
        for latitude in df['latitude'].dropna().unique():
            select_query = "SELECT latitude_id FROM gold_latitude WHERE latitude = %s"
            insert_query = "INSERT INTO gold_latitude (latitude) VALUES (%s)"
            self._get_or_create_id(select_query, insert_query, latitude)

    def _insert_phones(self, df):
        for phone in df['phone'].dropna().unique():
            select_query = "SELECT phone_id FROM gold_phone WHERE phone = %s"
            insert_query = "INSERT INTO gold_phone (phone) VALUES (%s)"
            self._get_or_create_id(select_query, insert_query, phone)

    def _insert_countries(self, df):
        for country in df['country'].dropna().unique():
            select_query = "SELECT country_id FROM gold_countries WHERE country_name = %s"
            insert_query = "INSERT INTO gold_countries (country_name) VALUES (%s)"
            self._get_or_create_id(select_query, insert_query, country)

    def _insert_breweries(self, df):
        for _, brewery in df.iterrows():
            state_id_query = "SELECT state_id FROM gold_states WHERE state_name = %s"
            self.cursor.execute(state_id_query, (brewery['state'],))
            state_id = self.cursor.fetchone()[0]

            city_id_query = "SELECT city_id FROM gold_cities WHERE city_name = %s AND state_id = %s"
            self.cursor.execute(city_id_query, (brewery['city'], state_id))
            city_id = self.cursor.fetchone()[0]

            country_id_query = "SELECT country_id FROM gold_countries WHERE country_name = %s"
            self.cursor.execute(country_id_query, (brewery['country'],))
            country_id = self.cursor.fetchone()[0]

            address_id_query = """
                SELECT address_id FROM gold_address WHERE address_1 = %s AND address_2 = %s AND address_3 = %s
            """
            self.cursor.execute(address_id_query, (brewery['address_1'], brewery['address_2'], brewery['address_3']))
            address_id = self.cursor.fetchone()[0]

            postal_code_id_query = "SELECT postal_code_id FROM gold_postal_code WHERE postal_code = %s"
            self.cursor.execute(postal_code_id_query, (brewery['postal_code'],))
            postal_code_id = self.cursor.fetchone()[0]

            longitude_id_query = "SELECT longitude_id FROM gold_longitude WHERE longitude = %s"
            self.cursor.execute(longitude_id_query, (brewery['longitude'],))
            longitude_id = self.cursor.fetchone()[0]

            latitude_id_query = "SELECT latitude_id FROM gold_latitude WHERE latitude = %s"
            self.cursor.execute(latitude_id_query, (brewery['latitude'],))
            latitude_id = self.cursor.fetchone()[0]

            phone_id_query = "SELECT phone_id FROM gold_phone WHERE phone = %s"
            self.cursor.execute(phone_id_query, (brewery['phone'],))
            phone_id = self.cursor.fetchone()[0]

            insert_query = """
                INSERT INTO gold_breweries (
                    brewery_id, name, city_id, state_id, country_id, address_id, postal_code_id,
                    longitude_id, latitude_id, phone_id, website_url
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (brewery_id) DO NOTHING
            """
            self.cursor.execute(insert_query, (
                brewery['id'], brewery['name'], city_id, state_id, country_id, address_id, postal_code_id,
                longitude_id, latitude_id, phone_id, brewery['website_url']
            ))

