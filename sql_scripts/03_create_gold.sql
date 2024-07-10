CREATE TABLE IF NOT EXISTS gold_states (
    state_id SERIAL PRIMARY KEY,
    state_name VARCHAR(100) UNIQUE
);

CREATE TABLE IF NOT EXISTS gold_address (
    address_id SERIAL PRIMARY KEY,
    address_1 VARCHAR(255),
    address_2 VARCHAR(255),
    address_3 VARCHAR(255),
    full_address TEXT
);

CREATE TABLE IF NOT EXISTS gold_postal_code (
    postal_code_id SERIAL PRIMARY KEY,
    postal_code VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS gold_cities (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) UNIQUE,
    state_id INTEGER REFERENCES gold_states(state_id)
);

CREATE TABLE IF NOT EXISTS gold_longitude (
    longitude_id SERIAL PRIMARY KEY,
    longitude VARCHAR(50) UNIQUE
);

CREATE TABLE IF NOT EXISTS gold_latitude (
    latitude_id SERIAL PRIMARY KEY,
    latitude VARCHAR(50) UNIQUE
);

CREATE TABLE IF NOT EXISTS gold_phone (
    phone_id SERIAL PRIMARY KEY,
    phone VARCHAR(50) UNIQUE
);

CREATE TABLE IF NOT EXISTS gold_countries (
    country_id SERIAL PRIMARY KEY,
    country_name VARCHAR(100) UNIQUE
);

CREATE TABLE IF NOT EXISTS gold_breweries (
    brewery_id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255),
    city_id INTEGER REFERENCES gold_cities(city_id),
    state_id INTEGER REFERENCES gold_states(state_id),
    country_id INTEGER REFERENCES gold_countries(country_id),
    address_id INTEGER REFERENCES gold_address(address_id),
    postal_code_id INTEGER REFERENCES gold_postal_code(postal_code_id),
    longitude_id INTEGER REFERENCES gold_longitude(longitude_id),
    latitude_id INTEGER REFERENCES gold_latitude(latitude_id),
    phone_id INTEGER REFERENCES gold_phone(phone_id),
    website_url VARCHAR(255)
);

