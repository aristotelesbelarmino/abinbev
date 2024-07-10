CREATE TABLE aggregated_breweries (
    id SERIAL PRIMARY KEY,
    brewery_type VARCHAR(255),
    state_province VARCHAR(255),
    brewery_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
