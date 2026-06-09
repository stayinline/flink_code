package org.example.job.state;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.dto.StateDemoEvent;

import java.util.Map;

/**
 * MapState 按课程字段聚合：每个 studentId 维护 courseId → 累计有效观看秒数。
 * <p>
 * RocksDB 下按 courseId 独立读写，不必像 ValueState+HashMap 那样整体反序列化整张 Map。
 */
public class CourseProgressMapFunction extends KeyedProcessFunction<String, StateDemoEvent, String> {

    private transient MapState<String, Long> courseWatchSecMap;

    @Override
    public void open(Configuration parameters) {
        courseWatchSecMap = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("course-watch-sec-map", String.class, Long.class));
    }

    @Override
    public void processElement(StateDemoEvent event,
                               Context ctx,
                               Collector<String> out) throws Exception {
        if (!StateDemoEvent.TYPE_VIDEO_PROGRESS.equals(event.getEventType())) {
            return;
        }

        String courseId = event.getCourseId();
        int watchSec = event.getWatchSec() != null ? event.getWatchSec() : 0;
        Long prev = courseWatchSecMap.get(courseId);
        long total = (prev != null ? prev : 0L) + watchSec;
        courseWatchSecMap.put(courseId, total);

        out.collect(String.format(
                "[MAP-AGG] subtask=%d studentId=%s courseId=%s | +%ds → total=%ds | mapSize=%d | tag=%s",
                getRuntimeContext().getIndexOfThisSubtask(),
                event.getStudentId(),
                courseId,
                watchSec,
                total,
                sizeOfMap(),
                event.getTag()));
    }

    private int sizeOfMap() throws Exception {
        int count = 0;
        for (Map.Entry<String, Long> ignored : courseWatchSecMap.entries()) {
            count++;
        }
        return count;
    }
}
