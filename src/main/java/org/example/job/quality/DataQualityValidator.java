package org.example.job.quality;

import org.example.dto.DirtyDataRecord;
import org.example.dto.StateDemoEvent;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;

/**
 * 脏数据识别规则（解析失败在 Ingress 层处理）。
 */
public final class DataQualityValidator {

    private static final Set<String> ALLOWED_EVENT_TYPES = Set.of(
            StateDemoEvent.TYPE_VIDEO_PROGRESS,
            StateDemoEvent.TYPE_QUIZ_ANSWER,
            StateDemoEvent.TYPE_QUIZ_QUESTION
    );

    /** 事件时间未来容忍（毫秒） */
    public static final long FUTURE_TOLERANCE_MS = 5 * 60 * 1000L;
    /** 事件时间过去最大容忍（毫秒，约 30 天） */
    public static final long PAST_TOLERANCE_MS = 30L * 24 * 60 * 60 * 1000;

    private DataQualityValidator() {
    }

    public static Optional<DirtyDataRecord> validate(StateDemoEvent event, String rawJson) {
        if (event.getStudentId() == null || event.getStudentId().isBlank()
                || event.getCourseId() == null || event.getCourseId().isBlank()
                || event.getEventType() == null || event.getEventType().isBlank()
                || event.getTs() == null) {
            return Optional.of(enrich(event, rawJson, DirtyDataRecord.REASON_MISSING_FIELD,
                    "studentId/courseId/eventType/ts 不能为空"));
        }

        if (!ALLOWED_EVENT_TYPES.contains(event.getEventType())) {
            return Optional.of(enrich(event, rawJson, DirtyDataRecord.REASON_INVALID_ENUM,
                    "eventType=" + event.getEventType()));
        }

        if (event.getTs() < 1_000_000_000_000L) {
            return Optional.of(enrich(event, rawJson, DirtyDataRecord.REASON_TIME_ANOMALY,
                    "ts 疑似秒级时间戳，应为毫秒"));
        }

        long now = Instant.now().toEpochMilli();
        if (event.getTs() > now + FUTURE_TOLERANCE_MS) {
            return Optional.of(enrich(event, rawJson, DirtyDataRecord.REASON_TIME_ANOMALY,
                    "事件时间超前超过 5 分钟"));
        }
        if (event.getTs() < now - PAST_TOLERANCE_MS) {
            return Optional.of(enrich(event, rawJson, DirtyDataRecord.REASON_TIME_ANOMALY,
                    "事件时间早于 30 天"));
        }

        if (StateDemoEvent.TYPE_VIDEO_PROGRESS.equals(event.getEventType())
                && (event.getEventId() == null || event.getEventId().isBlank())) {
            return Optional.of(enrich(event, rawJson, DirtyDataRecord.REASON_MISSING_BIZ_KEY,
                    "video_progress 缺少 eventId 业务主键"));
        }

        if (StateDemoEvent.TYPE_VIDEO_PROGRESS.equals(event.getEventType())
                && (event.getWatchSec() == null || event.getWatchSec() < 0)) {
            return Optional.of(enrich(event, rawJson, DirtyDataRecord.REASON_MISSING_FIELD,
                    "watchSec 非法"));
        }

        return Optional.empty();
    }

    private static DirtyDataRecord enrich(StateDemoEvent event, String rawJson,
                                          String reason, String detail) {
        DirtyDataRecord record = DirtyDataRecord.of(rawJson, reason, detail, event.getTag());
        record.setEventId(event.getEventId());
        record.setStudentId(event.getStudentId());
        if (event.getEventId() != null && !event.getEventId().isBlank()) {
            record.setDlqId("dlq-" + event.getEventId());
        }
        return record;
    }
}
