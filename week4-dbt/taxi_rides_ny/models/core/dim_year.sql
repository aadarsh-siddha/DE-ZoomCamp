SELECT DISTINCT EXTRACT(YEAR FROM pickup_datetime) AS pickup_year
FROM {{ ref('fact_trips') }}