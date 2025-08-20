package io.sentry.flink_bridge;

import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;

import java.util.Collections;
import java.util.Set;

import org.apache.flink.api.common.state.ValueStateDeclaration;
import org.apache.flink.api.common.state.ListStateDeclaration;
import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.state.StateDeclarations;
import org.apache.flink.api.common.state.v2.ListState;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.typeinfo.TypeDescriptors;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom processing function that adds logging after gRPC processing.
 * This implements the OneInputStreamProcessFunction pattern for Flink
 * DataStream API.
 */
public class CustomPostProcessor implements OneInputStreamProcessFunction<Message, Long> {

    private static final Logger LOG = LoggerFactory.getLogger(CustomPostProcessor.class);
    static final ValueStateDeclaration<Long> VALUE_STATE_DECLARATION = StateDeclarations
            .valueState("example-list-state", TypeDescriptors.LONG);

    // static final ValueStateDeclaration<Long> VALUE_STATE_DECLARATION =
    // StateDeclarations
    // .valueStateBuilder("example-list-state", TypeDescriptors.LONG).build();

    @Override
    public Set<StateDeclaration> usesStates() {
        return Collections.singleton(VALUE_STATE_DECLARATION);
    }

    @Override
    public void processRecord(Message record, Collector<Long> output, PartitionedContext<Long> ctx)
            throws Exception {

        ValueState<Long> state = ctx.getStateManager().getState(VALUE_STATE_DECLARATION);
        long stateVal = 0;
        if (state.value() != null) {
            stateVal = state.value();
        }
        state.update((long) stateVal + record.getPayload().length);
        stateVal = state.value();
        LOG.info("KEY {} Dumping message size: {}", ctx.getStateManager().getCurrentKey(), stateVal);
        output.collect(stateVal);
    }
}
