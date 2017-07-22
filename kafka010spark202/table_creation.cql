CREATE KEYSPACE utadproject WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor' : 1 };

USE utadproject;

DROP TABLE olaptaxis;
CREATE TABLE olaptaxis (
trip_date timestamp,
vendor_id int,
payment_type int,
avg_passenger_count float,
max_passenger_count int,
sum_passenger_count int,
avg_trip_distance float,
max_trip_distance float,
sum_trip_distance float,
avg_fare_amount float,
max_fare_amount float,
sum_fare_amount float,
avg_tip_amount float,
max_tip_amount float,
sum_tip_amount float,
sum_total_amount float,
avg_total_amount float,
max_total_amount float,
count_trips int,
PRIMARY KEY (trip_date, vendor_id, payment_type));

