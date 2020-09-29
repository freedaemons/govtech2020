CREATE USER docker;
CREATE DATABASE carsales;
GRANT ALL PRIVILEGES ON DATABASE carsales TO docker;

CREATE TABLE vehicle (
vehicle_id text PRIMARY KEY,
manufacturer text,
model_name text,
model_variant text,
serial_number text,
weight numeric,
engine_cubic_capacity numeric
)
;


CREATE TABLE vehicle_unit (
vehicle_unit_id text PRIMARY KEY,
vehicle_id text,
price numeric
)
;

CREATE TABLE transaction (
transaction_id text PRIMARY KEY,
vehicle_unit_id text,
salesperson_id text,
customer_id text
)
;

CREATE TABLE customer (
customer_id text PRIMARY KEY,
customer_name text,
customer_phone text
)
;