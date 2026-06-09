package org.example.job.state;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.dto.StateDemoEvent;

/**
 * ValueState 去重：按 eventId 标记是否已处理，过滤移动端重试导致的重复上报。
 */
public class DeduplicateFunction extends KeyedProcessFunction<String, StateDemoEvent, StateDemoEvent> {

    private transient ValueState<Boolean> seenFlag;

    @Override
    public void open(Configuration parameters) {
        seenFlag = getRuntimeContext().getState(
                new ValueStateDescriptor<>("dedup-seen-flag", Boolean.class));
    }

    @Override
    public void processElement(StateDemoEvent event,
                               Context ctx,
                               Collector<StateDemoEvent> out) throws Exception {
        Boolean seen = seenFlag.value();
        if (Boolean.TRUE.equals(seen)) {
            System.out.printf("[VALUE-DEDUP-SKIP] subtask=%d eventId=%s studentId=%s tag=%s | 重复上报已过滤%n",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    event.getEventId(),
                    event.getStudentId(),
                    event.getTag());
            return;
        }

        seenFlag.update(true);
        System.out.printf("[VALUE-DEDUP-PASS] subtask=%d eventId=%s studentId=%s eventType=%s tag=%s%n",
                getRuntimeContext().getIndexOfThisSubtask(),
                event.getEventId(),
                event.getStudentId(),
                event.getEventType(),
                event.getTag());
        out.collect(event);
    }
}
