with source as (
    select * from {{ source('raw', 'yellow_tripdata') }}
),

renamed as (
    select
        -- identifiers
        cast(vendorid as integer) as vendor_id,
        {{ safe_cast('ratecodeid', 'integer') }} as rate_code_id,
        cast(pulocationid as integer) as pickup_location_id,
        cast(dolocationid as integer) as dropoff_location_id,

        -- timestamps
        cast(tpep_pickup_datetime as timestamp) as pickup_datetime,  -- lpep = Licensed Passenger Enhancement Program (green taxis)
        cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,

        -- trip info
        cast(store_and_fwd_flag as string) as store_and_fwd_flag,
        cast(passenger_count as integer) as passenger_count,
        cast(trip_distance as numeric) as trip_distance,
        1 as trip_type, -- yellow taxis can only be street-hail (trip_type=1)

        -- payment info
        cast(fare_amount as numeric) as fare_amount,
        cast(extra as numeric) as extra,
        cast(mta_tax as numeric) as mta_tax,
        cast(tip_amount as numeric) as tip_amount,
        cast(tolls_amount as numeric) as tolls_amount,
        cast(improvement_surcharge as numeric) as improvement_surcharge,
        cast(0 as numeric) as ehail_fee, --yellow taxis do not have ehail fees
        cast(total_amount as numeric) as total_amount,
        {{ safe_cast('payment_type', 'integer') }} as payment_type
    from source
    -- Filter out records with null vendor_id (data quality requirement)
    where vendorid is not null
)

select * from renamed

-- Sample records for dev environment using deterministic date filter
{% if target.name == 'dev' %}
where pickup_datetime >= '2019-01-01' and pickup_datetime < '2019-02-01'
{% endif %}