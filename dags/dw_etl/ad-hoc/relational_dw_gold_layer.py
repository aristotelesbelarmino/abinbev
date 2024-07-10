import psycopg2
import traceback

class DataTransformer:
    def __init__(self):
        self.conn = psycopg2.connect(host='postgres', dbname='airflow', user='airflow', password='airflow')
        self.cursor = self.conn.cursor()

    def __del__(self):
        if self.conn is not None:
            self.conn.close()

    def transform_to_gold(self, **kwargs):
        try:
            self.cursor.execute("""SELECT brewery_id, name, city, state_province, country, address_1, address_2, address_3,
                postal_code, longitude, latitude, phone, website_url FROM silver_breweries""")
            rows = self.cursor.fetchall()

            states = self.get_gold_states()
            cities = self.get_gold_cities()
            countries = self.get_gold_countries()
            addresses = self.get_gold_address()
            postal_codes = self.get_gold_postal_code()
            longitudes = self.get_gold_longitude()
            latitudes = self.get_gold_latitude()
            phones = self.get_gold_phone()

            for row in rows:
                brewery_id, name, city, state_province, country, address_1, address_2, address_3, \
                postal_code, longitude, latitude, phone, website_url = row

                state_id = self._get_or_insert_state(state_province, states)
                country_id = self._get_or_insert_country(country, countries)
                address_id = self._get_or_insert_address(address_1, address_2, address_3, addresses)
                postal_code_id = self._get_or_insert_postal_code(postal_code, postal_codes)
                city_id = self._get_or_insert_city(city, state_id, cities)
                longitude_id = self._get_or_insert_longitude(longitude, longitudes)
                latitude_id = self._get_or_insert_latitude(latitude, latitudes)
                phone_id = self._get_or_insert_phone(phone, phones)

                self._insert_into_gold_breweries(brewery_id, name, city_id, state_id, country_id,
                                                 address_id, postal_code_id, longitude_id, latitude_id,
                                                 phone_id, website_url)

            self.conn.commit()
            print("Success! Layer: gold!")

        except (Exception, psycopg2.DatabaseError) as error:
            raise Exception(f"Error:\n{traceback.format_exc()}")
    
    def get_gold_states(self):
        self.cursor.execute("SELECT state_id, state_name FROM gold_states")
        rows = self.cursor.fetchall()
        state_info = {row[1]: row[0] for row in rows}

        return state_info

    def get_gold_address(self):
        try:
            self.cursor.execute("SELECT address_id, full_address FROM gold_address")
            rows = self.cursor.fetchall()
            address_info = {row[1]: row[0] for row in rows}
            return address_info

        except (Exception, psycopg2.DatabaseError) as error:
            raise Exception(f"Error:\n{traceback.format_exc()}")

    def get_gold_postal_code(self):
        try:
            self.cursor.execute("SELECT postal_code_id, postal_code FROM gold_postal_code")
            rows = self.cursor.fetchall()
            postal_code_info = {row[1]: row[0] for row in rows}
            return postal_code_info

        except (Exception, psycopg2.DatabaseError) as error:
            raise Exception(f"Error:\n{traceback.format_exc()}")

    def get_gold_cities(self):
        try:
            self.cursor.execute("SELECT city_id, city_name FROM gold_cities")
            rows = self.cursor.fetchall()
            city_info = {row[1]: row[0] for row in rows}
            return city_info

        except (Exception, psycopg2.DatabaseError) as error:
            raise Exception(f"Error:\n{traceback.format_exc()}")

    def get_gold_longitude(self):
        try:
            self.cursor.execute("SELECT longitude_id, longitude FROM gold_longitude")
            rows = self.cursor.fetchall()
            longitude_info = {row[1]: row[0] for row in rows}
            return longitude_info

        except (Exception, psycopg2.DatabaseError) as error:
            raise Exception(f"Error:\n{traceback.format_exc()}")

    def get_gold_latitude(self):
        try:
            self.cursor.execute("SELECT latitude_id, latitude FROM gold_latitude")
            rows = self.cursor.fetchall()
            latitude_info = {row[1]: row[0] for row in rows}
            return latitude_info

        except (Exception, psycopg2.DatabaseError) as error:
            raise Exception(f"Error:\n{traceback.format_exc()}")

    def get_gold_phone(self):
        try:
            self.cursor.execute("SELECT phone_id, phone FROM gold_phone")
            rows = self.cursor.fetchall()
            phone_info = {row[1]: row[0] for row in rows}
            return phone_info

        except (Exception, psycopg2.DatabaseError) as error:
            raise Exception(f"Error:\n{traceback.format_exc()}")

    def get_gold_countries(self):
        try:
            self.cursor.execute("SELECT country_id, country_name FROM gold_countries")
            rows = self.cursor.fetchall()
            country_info = {row[1]: row[0] for row in rows}
            return country_info

        except (Exception, psycopg2.DatabaseError) as error:
            raise Exception(f"Error:\n{traceback.format_exc()}")


    def _get_or_insert_state(self, state_province, states):
        if state_province not in states.keys():
            insert_state_query = "INSERT INTO gold_states (state_name) VALUES (%s) RETURNING state_id"
            self.cursor.execute(insert_state_query, (state_province,))
            state_id = self.cursor.fetchone()[0]
            states[state_province] = state_id
        else:
            state_id = states[state_province]
        return state_id

    def _get_or_insert_country(self, country, countries):
        if country not in countries.keys():
            insert_country_query = "INSERT INTO gold_countries (country_name) VALUES (%s) RETURNING country_id"
            self.cursor.execute(insert_country_query, (country,))
            country_id = self.cursor.fetchone()[0]
            countries[country] = country_id
        else:
            country_id = countries[country]
        return country_id

    def _get_or_insert_address(self, address_1, address_2, address_3, addresses):
        full_address = f"{address_1}, {address_2}, {address_3}" if address_2 or address_3 else address_1
        if full_address not in addresses.keys():
            insert_address_query = """
                INSERT INTO gold_address (address_1, address_2, address_3, full_address)
                VALUES (%s, %s, %s, %s) RETURNING address_id
            """
            self.cursor.execute(insert_address_query, (address_1, address_2, address_3, full_address))
            address_id = self.cursor.fetchone()[0]
            addresses[full_address] = address_id
        else:
            address_id = addresses[full_address]
        return address_id

    def _get_or_insert_postal_code(self, postal_code, postal_codes):
        if postal_code not in postal_codes.keys():
            insert_postal_code_query = "INSERT INTO gold_postal_code (postal_code) VALUES (%s) RETURNING postal_code_id"
            self.cursor.execute(insert_postal_code_query, (postal_code,))
            postal_code_id = self.cursor.fetchone()[0]
            postal_codes[postal_code] = postal_code_id
        else:
            postal_code_id = postal_codes[postal_code]
        return postal_code_id

    def _get_or_insert_city(self, city, state_id, cities):
        city_key = f"{city}_{state_id}"
        if city_key not in cities.keys():
            insert_city_query = "INSERT INTO gold_cities (city_name, state_id) VALUES (%s, %s) RETURNING city_id"
            self.cursor.execute(insert_city_query, (city, state_id))
            city_id = self.cursor.fetchone()[0]
            cities[city_key] = city_id
        else:
            city_id = cities[city_key]
        return city_id

    def _get_or_insert_longitude(self, longitude, longitudes):
        if longitude not in longitudes.keys():
            insert_longitude_query = "INSERT INTO gold_longitude (longitude) VALUES (%s) RETURNING longitude_id"
            self.cursor.execute(insert_longitude_query, (longitude,))
            longitude_id = self.cursor.fetchone()[0]
            longitudes[longitude] = longitude_id
        else:
            longitude_id = longitudes[longitude]
        return longitude_id

    def _get_or_insert_latitude(self, latitude, latitudes):
        if latitude not in latitudes.keys():
            insert_latitude_query = "INSERT INTO gold_latitude (latitude) VALUES (%s) RETURNING latitude_id"
            self.cursor.execute(insert_latitude_query, (latitude,))
            latitude_id = self.cursor.fetchone()[0]
            latitudes[latitude] = latitude_id
        else:
            latitude_id = latitudes[latitude]
        return latitude_id

    def _get_or_insert_phone(self, phone, phones):
        if phone not in phones.keys():
            insert_phone_query = "INSERT INTO gold_phone (phone) VALUES (%s) RETURNING phone_id"
            self.cursor.execute(insert_phone_query, (phone,))
            phone_id = self.cursor.fetchone()[0]
            phones[phone] = phone_id
        else:
            phone_id = phones[phone]
        return phone_id

    def _insert_into_gold_breweries(self, brewery_id, name, city_id, state_id, country_id,
                                    address_id, postal_code_id, longitude_id, latitude_id,
                                    phone_id, website_url):
        insert_brewery_query = """
            INSERT INTO gold_breweries (
                brewery_id, name, city_id, state_id, country_id, address_id,
                postal_code_id, longitude_id, latitude_id, phone_id, website_url
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """
        self.cursor.execute(insert_brewery_query, (
            brewery_id, name, city_id, state_id, country_id, address_id,
            postal_code_id, longitude_id, latitude_id, phone_id, website_url
        ))
