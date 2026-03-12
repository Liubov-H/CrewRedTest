CREATE TABLE dbo.sample_cab_data
(
    tpep_pickup_datetime   DATETIME2 NULL,
    tpep_dropoff_datetime  DATETIME2 NULL,
    passenger_count        TINYINT   NULL,
    trip_distance          DECIMAL(5,2) NULL,
    store_and_fwd_flag     NVARCHAR(3) NULL,
    PULocationID           NVARCHAR(3) NULL,
    DOLocationID           NVARCHAR(3) NULL,
    fare_amount            DECIMAL(5,2) NULL,
    tip_amount             DECIMAL(5,2) NULL,
    trip_duration AS DATEDIFF(SECOND, tpep_pickup_datetime, tpep_dropoff_datetime) PERSISTED
);

CREATE NONCLUSTERED INDEX IX_PULocationID_tip_amount
    ON dbo.sample_cab_data(PULocationID, tip_amount);

CREATE NONCLUSTERED INDEX IX_trip_distance
    ON dbo.sample_cab_data(trip_distance DESC);

CREATE NONCLUSTERED INDEX IX_trip_duration
    ON dbo.sample_cab_data(trip_duration DESC);

CREATE NONCLUSTERED INDEX IX_PULocationID
    ON dbo.sample_cab_data(PULocationID);