\c warehouse;
create extension if not exists "uuid-ossp";

CREATE TABLE dim_date (
	date_id int4 NOT NULL,
	date_actual date NOT NULL,
	day_suffix varchar(4) NOT NULL,
	day_name varchar(9) NOT NULL,
	day_of_year int4 NOT NULL,
	week_of_month int4 NOT NULL,
	week_of_year int4 NOT NULL,
	week_of_year_iso bpchar(10) NOT NULL,
	month_actual int4 NOT NULL,
	month_name varchar(9) NOT NULL,
	month_name_abbreviated bpchar(3) NOT NULL,
	quarter_actual int4 NOT NULL,
	quarter_name varchar(9) NOT NULL,
	year_actual int4 NOT NULL,
	first_day_of_week date NOT NULL,
	last_day_of_week date NOT NULL,
	first_day_of_month date NOT NULL,
	last_day_of_month date NOT NULL,
	first_day_of_quarter date NOT NULL,
	last_day_of_quarter date NOT NULL,
	first_day_of_year date NOT NULL,
	last_day_of_year date NOT NULL,
	mmyyyy bpchar(6) NOT NULL,
	mmddyyyy bpchar(10) NOT NULL,
	weekend_indr varchar(20) NOT NULL,
	CONSTRAINT dim_date_pkey PRIMARY KEY (date_id)
);

CREATE TABLE dim_time (
	time_id int4 NOT NULL,
	time_actual time NOT NULL,
	hours_24 bpchar(2) NOT NULL,
	hours_12 bpchar(2) NOT NULL,
	hour_minutes bpchar(2) NOT NULL,
	day_minutes int4 NOT NULL,
	day_time_name varchar(20) NOT NULL,
	day_night varchar(20) NOT NULL,
	CONSTRAINT time_pk PRIMARY KEY (time_id)
);

CREATE TABLE dim_bike (
    bike_id bigserial PRIMARY KEY,
    bike_nk INT UNIQUE NOT NULL,  -- Natural Key
    type VARCHAR,
    model VARCHAR,
    purchase_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_station (
    station_id bigserial PRIMARY KEY,
    station_nk INT UNIQUE NOT NULL,  -- Natural Key
    station_name VARCHAR,
    latitude NUMERIC,
    longitude NUMERIC,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_user_type (
    user_type_id bigserial PRIMARY KEY,
    user_type_nk INT UNIQUE NOT NULL,  -- Natural Key
    user_type_name VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE fact_trip_data (
    trip_id bigserial PRIMARY KEY,
    trip_duration INT,
    start_date INT,
    start_time INT,
    stop_date INT,
    stop_time INT,
    start_station_id bigint,
    end_station_id bigint,
    bike_id bigint,
    user_type_id bigint,
    year VARCHAR(255),
    month VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_start_station FOREIGN KEY (start_station_id) REFERENCES dim_station(station_id),
    CONSTRAINT fk_end_station FOREIGN KEY (end_station_id) REFERENCES dim_station(station_id),
    CONSTRAINT fk_bike FOREIGN KEY (bike_id) REFERENCES dim_bike(bike_id),
    CONSTRAINT fk_user_type FOREIGN KEY (user_type_id) REFERENCES dim_user_type(user_type_id),
    CONSTRAINT fk_start_date FOREIGN KEY (start_date) REFERENCES dim_date(date_id),
    CONSTRAINT fk_start_time FOREIGN KEY (start_time) REFERENCES dim_time(time_id),
    CONSTRAINT fk_stop_date FOREIGN KEY (stop_date) REFERENCES dim_date(date_id),
    CONSTRAINT fk_stop_time FOREIGN KEY (stop_time) REFERENCES dim_time(time_id)
);

CREATE TABLE fact_bike_usage (
    usage_id bigserial PRIMARY KEY,
    bike_id bigint,
    trip_count INT,
    total_duration INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_bike FOREIGN KEY (bike_id) REFERENCES dim_bike(bike_id)
);


insert into  dim_time
SELECT  
	cast(to_char(minute, 'hh24mi') as numeric) time_id,
	to_char(minute, 'hh24:mi')::time AS tume_actual,
	-- Hour of the day (0 - 23)
	to_char(minute, 'hh24') AS hour_24,
	-- Hour of the day (0 - 11)
	to_char(minute, 'hh12') hour_12,
	-- Hour minute (0 - 59)
	to_char(minute, 'mi') hour_minutes,
	-- Minute of the day (0 - 1439)
	extract(hour FROM minute)*60 + extract(minute FROM minute) day_minutes,
	-- Names of day periods
	case 
		when to_char(minute, 'hh24:mi') BETWEEN '00:00' AND '11:59'
		then 'AM'
		when to_char(minute, 'hh24:mi') BETWEEN '12:00' AND '23:59'
		then 'PM'
	end AS day_time_name,
	-- Indicator of day or night
	case 
		when to_char(minute, 'hh24:mi') BETWEEN '07:00' AND '19:59' then 'Day'	
		else 'Night'
	end AS day_night
FROM 
	(SELECT '0:00'::time + (sequence.minute || ' minutes')::interval AS minute 
	FROM  generate_series(0,1439) AS sequence(minute)
GROUP BY sequence.minute
) DQ
ORDER BY 1;


INSERT INTO dim_date
SELECT TO_CHAR(datum, 'yyyymmdd')::INT AS date_id,
       datum AS date_actual,
       TO_CHAR(datum, 'fmDDth') AS day_suffix,
       TO_CHAR(datum, 'TMDay') AS day_name,
       EXTRACT(DOY FROM datum) AS day_of_year,
       TO_CHAR(datum, 'W')::INT AS week_of_month,
       EXTRACT(WEEK FROM datum) AS week_of_year,
       EXTRACT(ISOYEAR FROM datum) || TO_CHAR(datum, '"-W"IW') AS week_of_year_iso,
       EXTRACT(MONTH FROM datum) AS month_actual,
       TO_CHAR(datum, 'TMMonth') AS month_name,
       TO_CHAR(datum, 'Mon') AS month_name_abbreviated,
       EXTRACT(QUARTER FROM datum) AS quarter_actual,
       CASE
           WHEN EXTRACT(QUARTER FROM datum) = 1 THEN 'First'
           WHEN EXTRACT(QUARTER FROM datum) = 2 THEN 'Second'
           WHEN EXTRACT(QUARTER FROM datum) = 3 THEN 'Third'
           WHEN EXTRACT(QUARTER FROM datum) = 4 THEN 'Fourth'
           END AS quarter_name,
       EXTRACT(YEAR FROM datum) AS year_actual,
       datum + (1 - EXTRACT(ISODOW FROM datum))::INT AS first_day_of_week,
       datum + (7 - EXTRACT(ISODOW FROM datum))::INT AS last_day_of_week,
       datum + (1 - EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
       (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
       DATE_TRUNC('quarter', datum)::DATE AS first_day_of_quarter,
       (DATE_TRUNC('quarter', datum) + INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-01-01', 'YYYY-MM-DD') AS first_day_of_year,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-12-31', 'YYYY-MM-DD') AS last_day_of_year,
       TO_CHAR(datum, 'mmyyyy') AS mmyyyy,
       TO_CHAR(datum, 'mmddyyyy') AS mmddyyyy,
       CASE
           WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN 'weekend'
           ELSE 'weekday'
           END AS weekend_indr
FROM (SELECT '1998-01-01'::DATE + SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES(0, 29219) AS SEQUENCE (DAY)
      GROUP BY SEQUENCE.DAY) DQ
ORDER BY 1;