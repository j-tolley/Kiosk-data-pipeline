-- This file should contain all code required to create & seed database tables.

-- psql postgres;
-- DROP DATABASE IF EXISTS museum;
-- CREATE DATABASE museum;
-- \c museum

CREATE TABLE IF NOT EXISTS exhibitions(
    exhibit_id INT GENERATED ALWAYS AS IDENTITY,
    exhibition_name TEXT NOT NULL,
    exhibition_id TEXT NOT NULL,
    site INT NOT NULL,
    floor TEXT NOT NULL,
    department TEXT NOT NULL,
    start_date date NOT NULL,
    description TEXT,
    PRIMARY KEY (exhibit_id),
    CONSTRAINT valid_exhibition_id CHECK (exhibition_id ILIKE 'EXH_%')
);

CREATE TABLE IF NOT EXISTS kiosk_output(
    kiosk_output_id INT GENERATED ALWAYS AS IDENTITY,
    exhibit_id INT,
    at timestamp NOT NULL,
    site INT NOT NULL,
    val INT NOT NULL CHECK (val in (-1, 0, 1, 2, 3, 4)),
    type INT CHECK (type IS NULL OR type IN (0, 1)),
    PRIMARY KEY (kiosk_output_id),
    FOREIGN KEY (exhibit_id) REFERENCES exhibitions(exhibit_id),
    CONSTRAINT type_button CHECK ( (val = -1 AND type IS NOT NULL) OR (val != -1 AND type IS NULL) )
);

CREATE INDEX idx_kiosk_duplicates 
ON kiosk_output(site, exhibit_id, val, type, at DESC);