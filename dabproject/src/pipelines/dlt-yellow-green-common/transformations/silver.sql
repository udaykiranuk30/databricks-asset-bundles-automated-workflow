-- A silver table with constraints which includes only valid data, Invalid data are dropped from the table,
--This table includes all the combined data of nyctaxi,paymenttypes,ratecodes,taxizones

create or refresh streaming table silver_nyctaxi.newyork_taxi_silver
(
  constraint valid_vendor_id expect(VendorID != 0) on violation drop row,
  constraint valid_date expect(pickup_datetime is not null and dropoff_datetime is not null and pickup_datetime < dropoff_datetime)on violation drop row,
  constraint valid_payment_type expect(PaymentTypeID != 0) on violation drop row,
  constraint valid_taxizone expect(LocationID != 0) on violation drop row,
  constraint valid_taxitype expect(taxitype != '') on violation drop row,
  constraint valid_passenger expect(passenger_count > 0) on violation drop row,
  constraint valid_trip_distance expect(trip_distance > 0) on violation drop row,
  constraint valid_ratecodeid expect(RatecodeID != 0) on violation drop row,
  constraint valid_totalamount expect (total_amount > 0) on violation drop row
)
as 
select
    yg.VendorID,yg.pickup_datetime,yg.dropoff_datetime,yg.passenger_count,yg.trip_distance,yg.total_amount,yg.taxitype,

    rc.RatecodeID,rc.RateCode,
    tz.LocationID,tz.Borough,
    pt.PaymentType,pt.PaymentTypeID

     from stream bronze_nyctaxi.yellowgreentaxi_bronze yg

     inner join stream bronze_nyctaxi.ratecodes_bronze rc
     on yg.RateCodeID=rc.RatecodeID

     inner join stream bronze_nyctaxi.taxizones_bronze tz
     on yg.DOLocationID=tz.LocationID

     inner join stream bronze_nyctaxi.paymenttypes_bronze pt
     on yg.payment_type=pt.PaymentTypeID;

