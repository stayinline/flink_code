package org.example.job.exactlyonce;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.dto.StateDemoEvent;
import org.example.dto.StudySummaryRecord;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 按学员聚合课程学习时长，向下游 Sink 输出汇总行。
 * <p>
 * 状态在 Checkpoint barrier 时快照 → Flink 内部 Exactly-Once 段保证。
 */
public class StudyProgressAggregateFunction
        extends KeyedProcessFunction<String, StateDemoEvent, StudySummaryRecord> {

    private static final ZoneId ZONE = ZoneId.of("Asia/Shanghai");
    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private transient MapState<String, Long> courseWatchSecMap;

    @Override
    public void open(Configuration parameters) {
        courseWatchSecMap = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("eo-study-course-watch", String.class, Long.class));
    }

    @Override
    public void processElement(StateDemoEvent event, Context ctx, Collector<StudySummaryRecord> out)
            throws Exception {
        if (!StateDemoEvent.TYPE_VIDEO_PROGRESS.equals(event.getEventType())) {
            return;
        }

        String courseId = event.getCourseId();
        int watchSec = event.getWatchSec() != null ? event.getWatchSec() : 0;
        Long prev = courseWatchSecMap.get(courseId);
        long total = (prev != null ? prev : 0L) + watchSec;
        courseWatchSecMap.put(courseId, total);

        String statDate = DATE_FMT.format(Instant.ofEpochMilli(event.getTs()).atZone(ZONE));
        StudySummaryRecord record = StudySummaryRecord.of(
                event.getEventId(),
                event.getStudentId(),
                courseId,
                total,
                statDate,
                event.getTag());

        System.out.printf("[EO-AGG] subtask=%d %s | +%ds%n",
                getRuntimeContext().getIndexOfThisSubtask(),
                record,
                watchSec);
        out.collect(record);
    }
}
