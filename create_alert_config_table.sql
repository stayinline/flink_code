-- 创建告警配置表
CREATE TABLE IF NOT EXISTS default.alert_config (
    id UInt32,
    rule_name String,
    metric_name String,
    min_value Float64,
    max_value Float64,
    alert_message String,
    enabled UInt8,
    update_time DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (id, metric_name)
PARTITION BY toDate(update_time);

-- 插入现有告警规则
INSERT INTO default.alert_config (id, rule_name, metric_name, min_value, max_value, alert_message, enabled) VALUES
(1, '温度异常', 'temperature', 10, 35, '温度超出正常范围[10, 35]°C', 1),
(2, '湿度异常', 'humidity', 30, 90, '湿度超出正常范围[30, 90]%', 1),
(3, 'CO2异常', 'co2', 400, 2000, 'CO2超出正常范围[400, 2000]ppm', 1),
(4, '土壤湿度异常', 'soil_moisture', 10, 80, '土壤湿度超出正常范围[10, 80]%', 1),
(5, '电池电量低', 'battery', 20, 100, '电池电量低于20%，请及时更换电池', 1),
(6, '设备离线', 'status', 0, 0, '设备状态为离线，请检查设备连接', 1);
