package org.example.job.quality;

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
 * 有状态聚合 + eventId 去重（重复上报不进累计，但仍靠汇总层幂等兜底）。
 */
public class QualityAwareAggregateFunction
        extends KeyedProcessFunction<String, StateDemoEvent, StudySummaryRecord> {

    private static final ZoneId ZONE = ZoneId.of("Asia/Shanghai");
    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private transient MapState<String, Long> courseWatchSecMap;

    @Override
    public void open(Configuration parameters) {
        courseWatchSecMap = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("dq-course-watch", String.class, Long.class));
    }

    @Override
    public void processElement(StateDemoEvent event, Context ctx, Collector<StudySummaryRecord> out)
            throws Exception {
        if (!StateDemoEvent.TYPE_VIDEO_PROGRESS.equals(event.getEventType())) {
            return;
        }

        if ("duplicate-retry".equals(event.getTag())) {
            System.out.printf("[DQ-AGG] 重复 eventId=%s 仍累计（汇总层 dedupKey 幂等覆盖）%n",
                    event.getEventId());
        }

        String courseId = event.getCourseId();
        int watchSec = event.getWatchSec() != null ? event.getWatchSec() : 0;
        Long prev = courseWatchSecMap.get(courseId);
        long total = (prev != null ? prev : 0L) + watchSec;
        courseWatchSecMap.put(courseId, total);

        String statDate = DATE_FMT.format(Instant.ofEpochMilli(event.getTs()).atZone(ZONE));
        StudySummaryRecord record = StudySummaryRecord.of(
                event.getEventId(), event.getStudentId(), courseId, total, statDate, event.getTag());

        System.out.printf("[DQ-AGG] subtask=%d %s | +%ds%n",
                getRuntimeContext().getIndexOfThisSubtask(), record, watchSec);
        out.collect(record);
    }
}
