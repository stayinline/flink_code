package org.example.job.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.dto.StateDemoEvent;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * ListState 缓存待处理答题：答题事件先于题目定义到达时先入队，题目到达后批量 flush。
 */
public class PendingQuizBufferFunction extends KeyedProcessFunction<String, StateDemoEvent, String> {

    private transient ListState<StateDemoEvent> pendingAnswers;

    @Override
    public void open(Configuration parameters) {
        pendingAnswers = getRuntimeContext().getListState(
                new ListStateDescriptor<>("pending-quiz-answers", StateDemoEvent.class));
    }

    @Override
    public void processElement(StateDemoEvent event,
                               Context ctx,
                               Collector<String> out) throws Exception {
        if (StateDemoEvent.TYPE_QUIZ_ANSWER.equals(event.getEventType())) {
            handleAnswer(event, out);
        } else if (StateDemoEvent.TYPE_QUIZ_QUESTION.equals(event.getEventType())) {
            handleQuestion(event, out);
        }
    }

    private void handleAnswer(StateDemoEvent event, Collector<String> out) throws Exception {
        pendingAnswers.add(event);
        int size = countPending();
        out.collect(String.format(
                "[LIST-BUFFER] subtask=%d studentId=%s questionId=%s score=%s | pendingSize=%d | tag=%s | 题目未到达，先缓存",
                getRuntimeContext().getIndexOfThisSubtask(),
                event.getStudentId(),
                event.getQuestionId(),
                event.getScore(),
                size,
                event.getTag()));
    }

    private void handleQuestion(StateDemoEvent event, Collector<String> out) throws Exception {
        String questionId = event.getQuestionId();
        List<StateDemoEvent> flushed = new ArrayList<>();
        List<StateDemoEvent> remaining = new ArrayList<>();

        for (StateDemoEvent pending : pendingAnswers.get()) {
            if (questionId != null && questionId.equals(pending.getQuestionId())) {
                flushed.add(pending);
            } else {
                remaining.add(pending);
            }
        }

        pendingAnswers.clear();
        for (StateDemoEvent item : remaining) {
            pendingAnswers.add(item);
        }

        out.collect(String.format(
                "[LIST-FLUSH] subtask=%d studentId=%s questionId=%s | flushed=%d remaining=%d | tag=%s",
                getRuntimeContext().getIndexOfThisSubtask(),
                event.getStudentId(),
                questionId,
                flushed.size(),
                remaining.size(),
                event.getTag()));

        for (StateDemoEvent answer : flushed) {
            out.collect(String.format(
                    "[LIST-MATCHED] studentId=%s questionId=%s score=%s eventId=%s",
                    answer.getStudentId(),
                    answer.getQuestionId(),
                    answer.getScore(),
                    answer.getEventId()));
        }
    }

    private int countPending() throws Exception {
        int count = 0;
        for (Iterator<StateDemoEvent> it = pendingAnswers.get().iterator(); it.hasNext(); ) {
            it.next();
            count++;
        }
        return count;
    }
}
