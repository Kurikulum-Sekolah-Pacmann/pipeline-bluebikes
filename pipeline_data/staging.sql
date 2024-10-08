\c staging;

CREATE TABLE public.bike (
    bike_id bigint NOT NULL,
    type text,
    model text,
    purchase_date date
);

CREATE TABLE public.station (
    station_id bigint NOT NULL,
    station_name text,
    latitude double precision,
    longitude double precision
);

CREATE TABLE public.user_type (
    user_type_id bigint NOT NULL,
    user_type_name text
);

CREATE TABLE trip_data_2020 (
   	trimduration INT,
    starttime TIMESTAMP,
    stoptime TIMESTAMP,
    start_station_id INT,
    start_station_name VARCHAR(255),
    start_station_latitude FLOAT,
    start_station_longitude FLOAT,
    end_station_id INT,
    end_station_name VARCHAR(255),
    end_station_latitude FLOAT,
    end_station_longitude FLOAT,
    bikeid INT,
    usertype VARCHAR(255),
    postal_code VARCHAR(255),
    year VARCHAR(255),
    month VARCHAR(255),
    birth_year VARCHAR(255),
    gender varchar(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
 );
 
CREATE TABLE trip_data_2019 (
   	trimduration INT,
    starttime TIMESTAMP,
    stoptime TIMESTAMP,
    start_station_id INT,
    start_station_name VARCHAR(255),
    start_station_latitude FLOAT,
    start_station_longitude FLOAT,
    end_station_id INT,
    end_station_name VARCHAR(255),
    end_station_latitude FLOAT,
    end_station_longitude FLOAT,
    bikeid INT,
    usertype VARCHAR(255),
    postal_code VARCHAR(255),
    year VARCHAR(255),
    month VARCHAR(255),
    birth_year VARCHAR(255),
    gender varchar(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
 );

CREATE VIEW combined_trip_data AS
SELECT 
    tripduration,
    starttime,
    stoptime,
    "start station id",
    "start station name",
    "start station latitude",
    "start station longitude",
    "end station id",
    "end station name",
    "end station latitude",
    "end station longitude",
    bikeid,
    usertype,
    "birth year",
    gender,
    NULL AS "postal code",  -- Adding NULL for missing column
    "year",
    "month",
    created_at
FROM trip_data_2019
WHERE usertype = 'Subscriber' AND tripduration::int > 500

UNION

SELECT 
    tripduration,
    starttime,
    stoptime,
    "start station id",
    "start station name",
    "start station latitude",
    "start station longitude",
    "end station id",
    "end station name",
    "end station latitude",
    "end station longitude",
    bikeid,
    usertype,
    "birth year",
    gender,
    "postal code",  -- Including the actual column
    "year",
    "month",
    created_at
FROM trip_data_2020
WHERE usertype = 'Subscriber' AND tripduration::int > 500;
