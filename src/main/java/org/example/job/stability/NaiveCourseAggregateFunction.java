package org.example.job.stability;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.dto.StateDemoEvent;

/**
 * 朴素按 courseId 聚合：热点课程全部落同一 subtask → 数据倾斜。
 */
public class NaiveCourseAggregateFunction
        extends KeyedProcessFunction<String, StateDemoEvent, String> {

    private transient ValueState<Long> totalWatchSec;
    private transient ValueState<Long> recordCount;

    @Override
    public void open(Configuration parameters) {
        totalWatchSec = getRuntimeContext().getState(
                new ValueStateDescriptor<>("skew-total-watch", Long.class));
        recordCount = getRuntimeContext().getState(
                new ValueStateDescriptor<>("skew-record-count", Long.class));
    }

    @Override
    public void processElement(StateDemoEvent event, Context ctx, Collector<String> out) throws Exception {
        if (!StateDemoEvent.TYPE_VIDEO_PROGRESS.equals(event.getEventType())) {
            return;
        }
        int watch = event.getWatchSec() != null ? event.getWatchSec() : 0;
        long total = (totalWatchSec.value() != null ? totalWatchSec.value() : 0L) + watch;
        long count = (recordCount.value() != null ? recordCount.value() : 0L) + 1;
        totalWatchSec.update(total);
        recordCount.update(count);

        boolean hot = StabilityConfigurator.HOT_COURSE_ID.equals(event.getCourseId());
        out.collect(String.format(
                "[SKEW-NAIVE] subtask=%d key=%s total=%ds count=%d hotKey=%s tag=%s",
                getRuntimeContext().getIndexOfThisSubtask(),
                ctx.getCurrentKey(),
                total,
                count,
                hot ? "YES⚠️" : "no",
                event.getTag()));
    }
}
