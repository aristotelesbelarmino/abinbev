import psycopg2, traceback, pyarrow as pa, pyarrow.parquet as pq, pandas as pd, os
from datetime import datetime

class DataLakeTransformer:
    def __init__(self):
        self.conn = psycopg2.connect(host='postgres', dbname='airflow', user='airflow', password='airflow')
        self.cursor = self.conn.cursor()

    def __del__(self):
        if self.conn is not None:
            self.conn.close()

    def transform_to_silver(self, **kwargs):
        try:
            select_query = "SELECT data FROM bronze_breweries ORDER BY load_date DESC LIMIT 1"
            self.cursor.execute(select_query)
            rows = self.cursor.fetchall()

            for row in rows:
                data = row[0]
                for entry in data:
                    self._write_to_parquet(entry)

            self.conn.commit()
            print("Success! Layer: Silver!")

        except (Exception, psycopg2.DatabaseError):
            raise Exception(f"Error on Silver Layer ETL:\n{traceback.format_exc()}")
        
    
    def _write_to_parquet(self, entry):
        date_extracted = datetime.now().strftime('%Y-%m-%d')
        file_path = f'/usr/local/airflow/data/silver/parquet_data/{date_extracted}'
        os.makedirs(file_path, exist_ok=True)

        df = pd.DataFrame([entry])
        df = df.where(pd.notnull(df), None)  

        table = pa.Table.from_pandas(df)
        pq.write_to_dataset(table, root_path=file_path, partition_cols=['state'])


   
    # In case we need a tabular source withn silver layer
    def _insert_into_silver_breweries(self, entry):
        insert_query = """
            INSERT INTO silver_breweries (
                brewery_id, name, brewery_type, address_1, address_2, address_3,
                city, state_province, postal_code, country, longitude, latitude,
                phone, website_url, state, street
            ) VALUES (
                %(id)s, %(name)s, %(brewery_type)s, %(address_1)s, %(address_2)s, %(address_3)s,
                %(city)s, %(state_province)s, %(postal_code)s, %(country)s, %(longitude)s, %(latitude)s,
                %(phone)s, %(website_url)s, %(state)s, %(street)s
            )
        """
        self.cursor.execute(insert_query, entry)

