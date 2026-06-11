package org.example.job.checkpoint;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.util.Collector;
import org.example.dto.StateDemoEvent;

import java.util.Map;

/**
 * 带状态的 keyed 算子：Checkpoint 时快照 MapState；实现 {@link CheckpointListener} 打印 CK 完成/中止。
 * <p>
 * 在 Flink UI → Checkpoints 页可对照本算子的 state size、sync/async duration、alignment time。
 */
public class StudyDurationCheckpointFunction
        extends KeyedProcessFunction<String, StateDemoEvent, String>
        implements CheckpointListener {

    private transient MapState<String, Long> courseWatchSecMap;
    private transient long lastCompletedCheckpointId = -1;

    @Override
    public void open(Configuration parameters) {
        courseWatchSecMap = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("ck-demo-course-watch-map", String.class, Long.class));
    }

    @Override
    public void processElement(StateDemoEvent event, Context ctx, Collector<String> out) throws Exception {
        if (!StateDemoEvent.TYPE_VIDEO_PROGRESS.equals(event.getEventType())) {
            return;
        }

        String courseId = event.getCourseId();
        int watchSec = event.getWatchSec() != null ? event.getWatchSec() : 0;
        Long prev = courseWatchSecMap.get(courseId);
        long total = (prev != null ? prev : 0L) + watchSec;
        courseWatchSecMap.put(courseId, total);

        out.collect(String.format(
                "[CK-STATE] subtask=%d studentId=%s courseId=%s | +%ds → total=%ds | mapEntries=%d "
                        + "| lastCkId=%d | tag=%s",
                getRuntimeContext().getIndexOfThisSubtask(),
                event.getStudentId(),
                courseId,
                watchSec,
                total,
                mapEntryCount(),
                lastCompletedCheckpointId,
                event.getTag()));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        lastCompletedCheckpointId = checkpointId;
        System.out.printf("[CK-COMPLETE] subtask=%d checkpointId=%d mapEntries≈%d%n",
                getRuntimeContext().getIndexOfThisSubtask(),
                checkpointId,
                safeMapEntryCount());
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        System.out.printf("[CK-ABORTED] subtask=%d checkpointId=%d（可能超时或对齐失败，查 UI alignment time）%n",
                getRuntimeContext().getIndexOfThisSubtask(),
                checkpointId);
    }

    private int mapEntryCount() throws Exception {
        int count = 0;
        for (Map.Entry<String, Long> ignored : courseWatchSecMap.entries()) {
            count++;
        }
        return count;
    }

    private int safeMapEntryCount() {
        try {
            return mapEntryCount();
        } catch (Exception e) {
            return -1;
        }
    }
}
