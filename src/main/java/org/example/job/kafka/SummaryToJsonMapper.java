package org.example.job.kafka;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.example.dto.StudySummaryRecord;

/**
 * 汇总记录序列化为 JSON，供 KafkaSink 写出（key = studentId 路由分区）。
 */
public class SummaryToJsonMapper extends RichMapFunction<StudySummaryRecord, String> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String map(StudySummaryRecord record) throws Exception {
        String json = objectMapper.writeValueAsString(record);
        System.out.printf("[KAFKA-SINK-IN] subtask=%d student=%s → %s%n",
                getRuntimeContext().getIndexOfThisSubtask(),
                record.getStudentId(),
                record.getDedupKey());
        return json;
    }
}
