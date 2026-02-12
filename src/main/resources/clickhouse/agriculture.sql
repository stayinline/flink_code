# ods层


CREATE TABLE default.ods_greenhouse_sensor_data
(

    `sensor_id` String,

    `greenhouse_id` String,

    `timestamp` UInt64,

    `location_x` Float64,

    `location_y` Float64,

    `location_z` Float64,

    `temperature` Float64,

    `humidity` Float64,

    `co2` UInt32,

    `light` UInt32,

    `soil_temperature` Float64,

    `soil_moisture` Float64,

    `device_battery` UInt8,

    `device_status` String,

    `device_firmware` String,

    `event_time` DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (greenhouse_id,
 sensor_id,
 timestamp)
SETTINGS index_granularity = 8192;

-- `default`.ods_greenhouse_soil_sensor_data definition

CREATE TABLE default.ods_greenhouse_soil_sensor_data
(

    `sensor_id` String COMMENT '土壤传感器ID',

    `sensor_type` String COMMENT '传感器类型(SOIL)',

    `greenhouse_id` String COMMENT '温室ID',

    `timestamp` UInt64 COMMENT '设备上报时间(ms)',

    `location_x` Float64,

    `location_y` Float64,

    `location_z` Float64,

    `soil_temperature` Float64 COMMENT '土壤温度(℃)',

    `soil_moisture` Float64 COMMENT '土壤湿度(%)',

    `soil_ec` Float64 COMMENT '土壤电导率(uS/cm)',

    `soil_ph` Float64 COMMENT '土壤PH值',

    `soil_n` Float64 COMMENT '氮含量(mg/kg)',

    `soil_p` Float64 COMMENT '磷含量(mg/kg)',

    `soil_k` Float64 COMMENT '钾含量(mg/kg)',

    `device_battery` UInt8 COMMENT '设备电量(%)',

    `device_status` String COMMENT '设备状态',

    `device_firmware` String COMMENT '固件版本',

    `event_time` DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (greenhouse_id,
 sensor_id,
 timestamp)
SETTINGS index_granularity = 8192;


-- `default`.ods_greenhouse_farm_operation definition

CREATE TABLE default.ods_greenhouse_farm_operation
(

    `operation_id` String COMMENT '农事操作ID',

    `data_type` String COMMENT '数据类型(MANUAL_OPERATION)',

    `greenhouse_id` String COMMENT '温室ID',

    `timestamp` UInt64 COMMENT '操作发生时间(ms)',

    `operator_id` String COMMENT '操作员ID',

    `operator_name` String COMMENT '操作员姓名',

    `operator_role` String COMMENT '角色',

    `operation_type` String COMMENT '操作类型(INSPECTION/IRRIGATION/FERTILIZATION)',

    `target_zone` String COMMENT '目标区域',

    `duration_sec` UInt32 COMMENT '操作持续时间(秒)',

    `irrigation_mode` Nullable(String),

    `water_volume_l` Nullable(Float64),

    `fertilizer_type` Nullable(String),

    `fertilizer_amount_kg` Nullable(Float64),

    `trigger_source` String COMMENT '触发来源(AI/MANUAL)',

    `decision_reason` String COMMENT '决策原因',

    `expected_effect` String COMMENT '预期效果',

    `snapshot_soil_moisture` Nullable(Float64),

    `snapshot_soil_ec` Nullable(Float64),

    `snapshot_soil_temperature` Nullable(Float64),

    `execution_status` String COMMENT '执行状态(SUCCESS/FAILED)',

    `audit_required` UInt8 COMMENT '是否需要审核',

    `remark` String COMMENT '备注',

    `event_time` DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (greenhouse_id,
 operation_type,
 timestamp)
SETTINGS index_granularity = 8192;


-- `default`.ods_greenhouse_plant_vision definition

CREATE TABLE default.ods_greenhouse_plant_vision
(

    `plant_detect_id` String COMMENT '植株识别事件ID',

    `greenhouse_id` String COMMENT '温室ID',

    `camera_id` String COMMENT '摄像头ID',

    `timestamp` UInt64 COMMENT '识别时间(ms)',

    `crop_type` String COMMENT '作物类型',

    `growth_stage` String COMMENT '生长阶段(SEEDLING/FLOWERING/FRUITING)',

    `plant_height_cm` Float64 COMMENT '植株高度(cm)',

    `leaf_count` UInt16 COMMENT '叶片数量',

    `canopy_coverage` Float64 COMMENT '冠层覆盖率(0-1)',

    `leaf_color_index` Float64 COMMENT '叶色指数',

    `chlorophyll_index` Float64 COMMENT '叶绿素指数',

    `wilting_score` Float64 COMMENT '萎蔫评分(0-1)',

    `disease_risk` String COMMENT '病害风险等级',

    `pest_risk` String COMMENT '虫害风险等级',

    `fruit_count` UInt16 COMMENT '果实数量',

    `avg_fruit_diameter_mm` Float64 COMMENT '平均果径(mm)',

    `fruit_color_stage` String COMMENT '果实颜色阶段',

    `water_stress` String COMMENT '水分胁迫',

    `nutrient_stress` String COMMENT '养分胁迫',

    `light_stress` String COMMENT '光照胁迫',

    `confidence` Float64 COMMENT '识别置信度',

    `model_version` String COMMENT '模型版本',

    `event_time` DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (greenhouse_id,
 timestamp)
SETTINGS index_granularity = 8192;

# DWD层

CREATE TABLE default.dwd_greenhouse_env_detail
(
    greenhouse_id String COMMENT '温室ID',
    sensor_id String COMMENT '环境传感器ID',
    ts DateTime COMMENT '事件时间',

    temperature Float64 COMMENT '空气温度',
    humidity Float64 COMMENT '空气湿度',
    co2 UInt32 COMMENT 'CO2浓度',
    light UInt32 COMMENT '光照强度',

    soil_temperature Float64 COMMENT '浅层土壤温度',
    soil_moisture Float64 COMMENT '浅层土壤湿度',

    device_status String COMMENT '设备状态'
)
ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (greenhouse_id, sensor_id, ts);

CREATE TABLE default.dwd_greenhouse_soil_detail
(
    greenhouse_id String COMMENT '温室ID',
    sensor_id String COMMENT '土壤传感器ID',
    ts DateTime COMMENT '事件时间',

    soil_temperature Float64 COMMENT '土壤温度',
    soil_moisture Float64 COMMENT '土壤湿度',
    soil_ec Float64 COMMENT '土壤电导率',
    soil_ph Float64 COMMENT '土壤PH',

    soil_n Float64 COMMENT '氮含量',
    soil_p Float64 COMMENT '磷含量',
    soil_k Float64 COMMENT '钾含量'
)
ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (greenhouse_id, sensor_id, ts);


CREATE TABLE default.dwd_greenhouse_plant_health_detail
(
    greenhouse_id String COMMENT '温室ID',
    ts DateTime COMMENT '识别时间',

    crop_type String COMMENT '作物类型',
    growth_stage String COMMENT '生长阶段',

    plant_height_cm Float64 COMMENT '植株高度',
    leaf_count UInt16 COMMENT '叶片数',
    canopy_coverage Float64 COMMENT '冠层覆盖率',

    chlorophyll_index Float64 COMMENT '叶绿素指数',
    wilting_score Float64 COMMENT '萎蔫评分',

    fruit_count UInt16 COMMENT '果实数',

    disease_risk String COMMENT '病害风险',
    pest_risk String COMMENT '虫害风险',

    soil_moisture Float64 COMMENT '同期土壤湿度',
    soil_ec Float64 COMMENT '同期土壤电导率',
    soil_ph Float64 COMMENT '同期土壤PH',

    temperature Float64 COMMENT '同期环境温度',
    humidity Float64 COMMENT '同期环境湿度'
)
ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (greenhouse_id, ts);






# DWS 层

CREATE TABLE default.dws_greenhouse_crop_health_snapshot
(
    greenhouse_id String COMMENT '温室ID',
    stat_time DateTime COMMENT '统计时间',

    crop_type String COMMENT '作物类型',
    growth_stage String COMMENT '生长阶段',

    avg_plant_height Float64 COMMENT '平均植株高度',
    avg_leaf_count Float64 COMMENT '平均叶片数',
    avg_chlorophyll Float64 COMMENT '平均叶绿素指数',

    avg_soil_moisture Float64 COMMENT '平均土壤湿度',
    avg_soil_ec Float64 COMMENT '平均电导率',

    disease_risk_rate Float64 COMMENT '病害风险比例',
    pest_risk_rate Float64 COMMENT '虫害风险比例',

    last_operation_type String COMMENT '最近一次农事操作',
    last_operation_time DateTime COMMENT '最近操作时间'
)
ENGINE = MergeTree
PARTITION BY toDate(stat_time)
ORDER BY (greenhouse_id, stat_time);
