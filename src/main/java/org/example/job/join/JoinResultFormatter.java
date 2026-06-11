package org.example.job.join;

import org.apache.flink.api.common.functions.MapFunction;
import org.example.dto.JoinResult;

/**
 * 格式化 Join 输出日志。
 */
public class JoinResultFormatter implements MapFunction<JoinResult, String> {

    @Override
    public String map(JoinResult r) {
        return String.format(
                "[JOIN-OUT] type=%s requestId=%s student=%s exposure=%s click=%s "
                        + "expTs=%s clickTs=%s delayMs=%s | %s",
                r.getJoinType(),
                r.getRequestId(),
                r.getStudentId(),
                r.getExposureId(),
                r.getClickId(),
                r.getExposureTs(),
                r.getClickTs(),
                r.getDelayMs(),
                r.getNote());
    }
}
