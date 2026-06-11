package org.example.job.quality;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.example.dto.DirtyDataRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * DLQ 落库模拟：内存存储 + 日志，生产可换 Kafka topic / Hive 外表 / ClickHouse。
 */
public class DlqSinkFunction extends RichSinkFunction<DirtyDataRecord> {

    public static final CopyOnWriteArrayList<DirtyDataRecord> DLQ_STORE = new CopyOnWriteArrayList<>();

    @Override
    public void open(Configuration parameters) {
        System.out.println("[DLQ-SINK] 就绪 | 表字段: dlqId, reason, rawPayload, replayVersion, detectedAtMs");
    }

    @Override
    public void invoke(DirtyDataRecord value, Context context) {
        DLQ_STORE.add(value);
        System.out.printf("[DLQ-SINK] dlqId=%s reason=%-16s replayable=%s | %s%n",
                value.getDlqId(), value.getReason(), value.isReplayable(), value.getDetail());
    }

    public static List<DirtyDataRecord> replayableRecords() {
        List<DirtyDataRecord> list = new ArrayList<>();
        for (DirtyDataRecord r : DLQ_STORE) {
            if (r.isReplayable()) {
                list.add(r);
            }
        }
        return list;
    }

    public static void clearStore() {
        DLQ_STORE.clear();
    }
}
