-- models/simple_model.sql
select
    station_id,
    name,
    current_date() as load_date
from
    bigquery-public-data.new_york_citibike.citibike_stations
where
    rental_methods = 'KEY' -- Corrected from rental_method