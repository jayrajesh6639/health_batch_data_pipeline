CREATE OR REPLACE TABLE `fleet-reserve-464105-i4.global_transform_ds.global_health_data_{country}` AS
SELECT * FROM `fleet-reserve-464105-i4.global_stg_ds.global_stg_tbl`
WHERE country = '{country}';
