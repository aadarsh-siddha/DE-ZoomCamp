SELECT DISTINCT
    EXTRACT(YEAR FROM pickup_datetime) AS year,
    CONCAT('Q', CAST(EXTRACT(QUARTER FROM pickup_datetime) as string)) AS quarter
FROM {{ ref("fact_trips") }}