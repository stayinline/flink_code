package org.example.job.stability;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.dto.CourseWatchPartial;
import org.example.dto.StateDemoEvent;

/**
 * 两阶段聚合 — Local 阶段：key = salt|courseId，打散热点 key 到多个 subtask。
 */
public class LocalSaltedAggregateFunction
        extends KeyedProcessFunction<String, StateDemoEvent, CourseWatchPartial> {

    private final int saltBuckets;

    private transient ValueState<Long> partialWatchSec;
    private transient ValueState<Long> partialCount;

    public LocalSaltedAggregateFunction(int saltBuckets) {
        this.saltBuckets = saltBuckets;
    }

    static String saltedKey(String courseId, String studentId, int saltBuckets) {
        int salt = Math.floorMod(studentId.hashCode(), saltBuckets);
        return salt + "|" + courseId;
    }

    static int extractSalt(String saltedKey) {
        return Integer.parseInt(saltedKey.split("\\|", 2)[0]);
    }

    static String extractCourseId(String saltedKey) {
        return saltedKey.split("\\|", 2)[1];
    }

    @Override
    public void open(Configuration parameters) {
        partialWatchSec = getRuntimeContext().getState(
                new ValueStateDescriptor<>("local-partial-sec", Long.class));
        partialCount = getRuntimeContext().getState(
                new ValueStateDescriptor<>("local-partial-count", Long.class));
    }

    @Override
    public void processElement(StateDemoEvent event, Context ctx, Collector<CourseWatchPartial> out)
            throws Exception {
        if (!StateDemoEvent.TYPE_VIDEO_PROGRESS.equals(event.getEventType())) {
            return;
        }
        int watch = event.getWatchSec() != null ? event.getWatchSec() : 0;
        long total = (partialWatchSec.value() != null ? partialWatchSec.value() : 0L) + watch;
        long count = (partialCount.value() != null ? partialCount.value() : 0L) + 1;
        partialWatchSec.update(total);
        partialCount.update(count);

        String saltedKey = ctx.getCurrentKey();
        int salt = extractSalt(saltedKey);
        String courseId = extractCourseId(saltedKey);

        out.collect(new CourseWatchPartial(courseId, salt, total, count, event.getTag()));

        System.out.printf(
                "[2PHASE-LOCAL] subtask=%d saltedKey=%s partialSec=%d count=%d tag=%s%n",
                getRuntimeContext().getIndexOfThisSubtask(),
                saltedKey,
                total,
                count,
                event.getTag());
    }
}
