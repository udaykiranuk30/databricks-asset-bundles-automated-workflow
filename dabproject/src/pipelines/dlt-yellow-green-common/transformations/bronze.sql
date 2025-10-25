--ingesting raw yellowtaxi data
create or refresh streaming table bronze_nyctaxi.yellowtaxi_bronze
as
select VendorID,
    tpep_pickup_datetime as pickup_datetime,
    tpep_dropoff_datetime as dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    current_timestamp() as processing_time,
    _metadata.file_path as file_path
    from stream read_files(
        "${source}/yellow",
        format => 'csv',
        inferSchema => true,
        header => true
    );

    --ingesting raw greentaxi data
create or refresh streaming table bronze_nyctaxi.greentaxi_bronze
    as
    select VendorID,
    lpep_pickup_datetime as pickup_datetime,
    lpep_dropoff_datetime as dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID,
    PULocationID,
    DOLocationID,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge,
    current_timestamp() as processing_time,
    _metadata.file_path as file_path
    from stream read_files(
        "${source}/green",
        format => 'csv',
        inferSchema => true,
        header => true
    );
-- Combining yellow taxi and green taxi data on required columns 
create or refresh streaming table bronze_nyctaxi.yellowgreentaxi_bronze
    as 
    select 
    VendorID,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    DOLocationID,
    payment_type,
    total_amount,
    "Yellow" as taxitype
    from stream bronze_nyctaxi.yellowtaxi_bronze
    union all
    select 
    VendorID,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    DOLocationID,
    payment_type,
    total_amount,
    "Green" as taxitype
    from stream bronze_nyctaxi.greentaxi_bronze;

--ingesting paymenttypes data
create or refresh streaming table bronze_nyctaxi.paymenttypes_bronze
    as select *,current_timestamp() as processing_time,
    _metadata.file_path as file_path
    from stream read_files(
        "${source}/common/PaymentTypes",
        format => 'json',
        inferSchema => true
        );

--ingesting ratecodes data
create or refresh streaming table bronze_nyctaxi.ratecodes_bronze
    as select *,current_timestamp() as processing_time,
    _metadata.file_path as file_path
    from stream read_files(
        "${source}/common/RateCodes",
        format => 'csv',
        inferSchema => true,
        header => true
        );

--ingesting TaxiZones Data
create or refresh streaming table bronze_nyctaxi.taxizones_bronze
    as select *,current_timestamp() as processing_time,
    _metadata.file_path as file_path
    from stream read_files(
        "${source}/common/TaxiZones",
        format => 'csv',
        inferSchema => true,
        header => true
        );
    


