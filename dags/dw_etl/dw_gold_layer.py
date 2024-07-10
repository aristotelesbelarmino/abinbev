import pyarrow.parquet as pq, pandas as pd, psycopg2, traceback, os
from datetime import datetime, timedelta


class DataLakeReader:
    def __init__(self):
        self.conn = psycopg2.connect(host='postgres', dbname='airflow', user='airflow', password='airflow')
        self.cursor = self.conn.cursor()

    def __del__(self):
        if self.conn is not None:
            self.conn.close()

    def aggregate_breweries(self, **kwargs):
        try:
            latest_date = self._find_latest_date()
            file_path = f'/usr/local/airflow/data/silver/parquet_data/{latest_date}'
            df = self._read_parquet(file_path)
            aggregated_df = self._aggregate_data(df)
            self._insert_aggregated_data(aggregated_df)
            print("Aggregated data inserted successfully!")

        except Exception as e:
            raise Exception(f"Error aggregating data:\n{traceback.format_exc()}")

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
        return combined_df
    
    def _aggregate_data(self, df):
        aggregated_df = df.groupby(['brewery_type', 'state_province']).size().reset_index(name='brewery_count')
        return aggregated_df

    def _insert_aggregated_data(self, df):
        for index, row in df.iterrows():
            insert_query = """
                INSERT INTO aggregated_breweries (
                    brewery_type, state_province, brewery_count
                ) VALUES (
                    %s, %s, %s
                )
            """
            self.cursor.execute(insert_query, (row['brewery_type'], row['state_province'], row['brewery_count']))
        self.conn.commit()