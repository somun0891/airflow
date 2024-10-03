USE ROLE TRANSFORM;
USE WAREHOUSE COMPUTE_WH;
select '{{ ds }}' as logical_date,
'{{ data_interval_start }}' as data_interval_start,
'{{ ds_nodash }}' as formatted_logical_date,
 '{{ params.env }}' as Environment --airflow variable access using conf
;