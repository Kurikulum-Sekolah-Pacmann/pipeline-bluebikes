\c staging;

CREATE TABLE public.bike (
    bike_id bigint NOT NULL,
    type text,
    model text,
    purchase_date date,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE public.station (
    station_id bigint NOT NULL,
    station_name text,
    latitude float,
    longitude float,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE public.user_type (
    user_type_id bigint NOT NULL,
    user_type_name text,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE trip_data_2020 (
    tripduration VARCHAR,
    starttime VARCHAR,
    stoptime VARCHAR,
    "start station id" VARCHAR,
    "start station name" VARCHAR(255),
    "start station latitude" VARCHAR,
    "start station longitude" VARCHAR,
    "end station id" VARCHAR,
    "end station name" VARCHAR(255),
    "end station latitude" VARCHAR,
    "end station longitude" VARCHAR,
    bikeid VARCHAR,
    usertype VARCHAR(255),
    "postal code" VARCHAR(255),
    year VARCHAR(10),
    month VARCHAR(10),
    "birth year" VARCHAR(10),
    gender VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE trip_data_2019 (
    tripduration VARCHAR,
    starttime VARCHAR,
    stoptime VARCHAR,
    "start station id" VARCHAR,
    "start station name" VARCHAR(255),
    "start station latitude" VARCHAR,
    "start station longitude" VARCHAR,
    "end station id" VARCHAR,
    "end station name" VARCHAR(255),
    "end station latitude" VARCHAR,
    "end station longitude" VARCHAR,
    bikeid VARCHAR,
    usertype VARCHAR(255),
    "postal code" VARCHAR(255),
    year VARCHAR(10),
    month VARCHAR(10),
    "birth year" VARCHAR(10),
    gender VARCHAR(10),
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
    NULL AS "postal code",
    year,
    month,
    created_at
FROM trip_data_2019
WHERE usertype = 'Subscriber' AND tripduration::int > 500

UNION ALL

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
    "postal code",
    year,
    month,
    created_at
FROM trip_data_2020
WHERE usertype = 'Subscriber' AND tripduration::int > 500;
