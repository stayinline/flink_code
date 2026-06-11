package org.example.job.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.example.dto.EducationClickEvent;
import org.example.dto.EducationExposureEvent;
import org.example.dto.JoinResult;

public class EducationWindowJoinFunction
        implements JoinFunction<EducationExposureEvent, EducationClickEvent, JoinResult> {

    @Override
    public JoinResult join(EducationExposureEvent exposure, EducationClickEvent click) {
        return JoinResult.matched(
                "WINDOW_JOIN",
                exposure,
                click,
                "same requestId and same tumbling event-time window");
    }
}
