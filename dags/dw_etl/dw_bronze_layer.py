import os, requests, json, psycopg2

class ExtractData:
    def __init__(self):
        self.conn = psycopg2.connect(host='postgres', dbname='airflow', user='airflow', password='airflow')
        self.cursor = self.conn.cursor()

    def __del__(self):
        if self.conn is not None:
            self.conn.close()
            print("Conexão com o PostgreSQL fechada.")

    def extract_data(self, **kwargs):
        url = "https://api.openbrewerydb.org/breweries"
        response = requests.get(url)
        data = response.json()
        
        os.makedirs('/usr/local/airflow/data/bronze/',exist_ok=1)
        with open(f"/usr/local/airflow/data/bronze/{kwargs['ds']}.json", 'w') as f:
            json.dump(data, f)

        self.cursor.execute("INSERT INTO bronze_breweries (data) VALUES (%s)", (json.dumps(data),))
        self.conn.commit()

