package org.example.job.savepoint;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.dto.StateDemoEvent;

import java.util.Map;

/**
 * 在线教育「学员课程学分/有效观看时长」累计算子。
 * <p>
 * 状态：{@code MapState<courseId, totalWatchSec>}，描述符名 {@link #STATE_DESCRIPTOR_NAME} 在 V1/V2 间保持不变以支持 Savepoint 恢复。
 * <p>
 * V2 仅增加业务逻辑（promotion 标签 +10% 加成），不改变状态 schema —— 典型<strong>兼容升级</strong>。
 */
public class CourseCreditAccumulateFunction
        extends KeyedProcessFunction<String, StateDemoEvent, String> {

    /** Savepoint 恢复依赖：描述符名称 + 类型必须兼容 */
    public static final String STATE_DESCRIPTOR_NAME = "sp-demo-course-credit-map";

    public enum JobVersion {
        V1, V2
    }

    private final JobVersion jobVersion;

    private transient MapState<String, Long> courseCreditMap;

    public CourseCreditAccumulateFunction(JobVersion jobVersion) {
        this.jobVersion = jobVersion != null ? jobVersion : JobVersion.V1;
    }

    @Override
    public void open(Configuration parameters) {
        courseCreditMap = getRuntimeContext().getMapState(
                new MapStateDescriptor<>(STATE_DESCRIPTOR_NAME, String.class, Long.class));
    }

    @Override
    public void processElement(StateDemoEvent event, Context ctx, Collector<String> out) throws Exception {
        if (!StateDemoEvent.TYPE_VIDEO_PROGRESS.equals(event.getEventType())) {
            return;
        }

        String courseId = event.getCourseId();
        int watchSec = event.getWatchSec() != null ? event.getWatchSec() : 0;
        long credited = applyVersionLogic(watchSec, event.getTag());

        Long prev = courseCreditMap.get(courseId);
        long total = (prev != null ? prev : 0L) + credited;
        courseCreditMap.put(courseId, total);

        out.collect(String.format(
                "[SP-STATE] subtask=%d version=%s studentId=%s courseId=%s | raw=%ds credited=%ds → total=%ds "
                        + "| mapEntries=%d | tag=%s",
                getRuntimeContext().getIndexOfThisSubtask(),
                jobVersion.name(),
                event.getStudentId(),
                courseId,
                watchSec,
                credited,
                total,
                mapEntryCount(),
                event.getTag()));
    }

    private long applyVersionLogic(int watchSec, String tag) {
        if (jobVersion == JobVersion.V2 && tag != null && tag.contains("promotion")) {
            // V2 新逻辑：促销课额外 +10% 学分（状态仍为 Long，Savepoint 可恢复）
            return watchSec + Math.round(watchSec * 0.1);
        }
        return watchSec;
    }

    private int mapEntryCount() throws Exception {
        int count = 0;
        for (Map.Entry<String, Long> ignored : courseCreditMap.entries()) {
            count++;
        }
        return count;
    }
}
