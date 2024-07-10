CREATE TABLE IF NOT EXISTS silver_breweries (
    brewery_id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255),
    brewery_type VARCHAR(50),
    address_1 VARCHAR(255),
    address_2 VARCHAR(255),
    address_3 VARCHAR(255),
    city VARCHAR(100),
    state_province VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(100),
    longitude VARCHAR(50),
    latitude VARCHAR(50),
    phone VARCHAR(20),
    website_url VARCHAR(255),
    state VARCHAR(100),
    street VARCHAR(255)
);
