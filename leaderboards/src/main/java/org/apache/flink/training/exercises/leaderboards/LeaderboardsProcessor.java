package org.apache.flink.training.exercises.leaderboards;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.training.exercises.common.datatypes.GameEvent;
import org.apache.flink.training.exercises.common.sources.GameEventGenerator;
import org.apache.flink.training.exercises.common.utils.ExerciseBase;

/**
 * Flink job to process game event data into leaderboards.
 *
 */
public class LeaderboardsProcessor extends ExerciseBase {
    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {
        // set up stream env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // start the data generator
        DataStream<GameEvent> gameEvent = env.addSource(gameEventSourceOrTest(new GameEventGenerator()));
        printOrTest(gameEvent);

        env.execute("Game Events to Leaderboards!");
    }
}