package org.example.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.sql.Timestamp;

// 定义数据模型类
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KafkaData implements Serializable {
    private String id;
    private String message;
    private Double value;
    @JsonProperty("event_time")
    private String eventTime;


}