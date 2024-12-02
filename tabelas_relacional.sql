CREATE TABLE countries (
    country_id SERIAL PRIMARY KEY,
    country_name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE platforms (
    platform_id SERIAL PRIMARY KEY,
    platform_name VARCHAR(50) UNIQUE NOT NULL
);

CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    text TEXT,
    sentiment VARCHAR(50),
    timestamp TIMESTAMP,
    user_name VARCHAR(50),
    platform_id INT REFERENCES platforms(platform_id),
    hashtags TEXT,
    retweets INT,
    likes INT,
    country_id INT REFERENCES countries(country_id),
    year INT,
    month INT,
    day INT,
    hour INT
);

ALTER TABLE
    countries
ADD
    CONSTRAINT unique_country_name UNIQUE (country_name);

ALTER TABLE
    platforms
ADD
    CONSTRAINT unique_platform_name UNIQUE (platform_name);