-- 创建 ClickHouse 表结构
CREATE TABLE IF NOT EXISTS default.greenhouse_sensor_data (
    sensor_id String,
    greenhouse_id String,
    timestamp UInt64,
    location_x Float64,
    location_y Float64,
    location_z Float64,
    temperature Float64,
    humidity Float64,
    co2 UInt32,
    light UInt32,
    soil_temperature Float64,
    soil_moisture Float64,
    device_battery UInt8,
    device_status String,
    device_firmware String,
    event_time DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (greenhouse_id, sensor_id, timestamp)
PARTITION BY toDate(event_time);
