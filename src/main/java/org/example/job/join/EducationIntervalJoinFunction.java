package org.example.job.join;

import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;
import org.example.dto.EducationClickEvent;
import org.example.dto.EducationExposureEvent;
import org.example.dto.JoinResult;

public class EducationIntervalJoinFunction
        extends ProcessJoinFunction<EducationExposureEvent, EducationClickEvent, JoinResult> {

    @Override
    public void processElement(
            EducationExposureEvent exposure,
            EducationClickEvent click,
            Context ctx,
            Collector<JoinResult> out) {
        out.collect(
                JoinResult.matched(
                        "INTERVAL_JOIN",
                        exposure,
                        click,
                        "click.ts in [exposure.ts, exposure.ts + upperBound]"));
        System.out.printf(
                "[INTERVAL-JOIN] requestId=%s exposure=%s click=%s delayMs=%d%n",
                exposure.getRequestId(),
                exposure.getExposureId(),
                click.getClickId(),
                click.getTs() - exposure.getTs());
    }
}
