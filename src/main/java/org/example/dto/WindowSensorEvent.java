package org.example.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * 窗口演示用传感器事件，JSON 示例：
 * {"sensorId":"sensor-01","value":10.5,"eventTime":1717654321000}
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class WindowSensorEvent implements Serializable {

    private String sensorId;
    private Double value;
    @JsonProperty("eventTime")
    private Long eventTime;

    @Override
    public String toString() {
        return String.format("WindowSensorEvent{sensorId='%s', value=%s, eventTime=%d}",
                sensorId, value, eventTime);
    }
}
