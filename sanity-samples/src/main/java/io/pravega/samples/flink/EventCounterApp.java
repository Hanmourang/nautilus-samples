/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.samples.flink;

import io.pravega.client.stream.*;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.PravegaWriterMode;
import io.pravega.connectors.flink.PravegaEventRouter;
import io.pravega.connectors.flink.serialization.PravegaSerialization;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

import static io.pravega.samples.flink.EventCounterApp.NotifyingMapper.TO_CALL_ON_COMPLETION;

public class EventCounterApp {

	private static final Logger log = LoggerFactory.getLogger(EventCounterApp.class);

	public static final int DEFAULT_PARALLELISM = 1;

	private final int parallelism;

	public EventCounterApp() {
		this(DEFAULT_PARALLELISM);
	}

	public EventCounterApp(int parallelism) {
		this.parallelism = parallelism;
	}

	public void standardReadWriteSimulator(final Stream inStream, final Stream outStream, final StreamUtils streamUtils,
										   int numElements, PravegaConfig pravegaConfig) throws Exception {

		final int checkpointInterval = 100;
		final int taskFailureRestartAttempts = 1;
		final long delayBetweenRestartAttempts = 0L;
		final String jobName = "standardReadWriteSimulator";

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);
		env.enableCheckpointing(checkpointInterval);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(taskFailureRestartAttempts, delayBetweenRestartAttempts));

		// the Pravega reader
		final FlinkPravegaReader<Integer> pravegaSource = FlinkPravegaReader.<Integer>builder()
				.withPravegaConfig(pravegaConfig)
				.forStream(inStream)
				.withDeserializationSchema(PravegaSerialization.deserializationFor(Integer.class))
				.build();

		// Pravega Writer
		FlinkPravegaWriter<Integer> pravegaWriter = FlinkPravegaWriter.<Integer>builder()
				.withPravegaConfig(pravegaConfig)
				.forStream(outStream)
				.withSerializationSchema(PravegaSerialization.serializationFor(Integer.class))
				.withEventRouter(new IdentityRouter<Integer>())
				.withWriterMode(PravegaWriterMode.ATLEAST_ONCE)
				.build();

		DataStream<Integer> stream = env.addSource(pravegaSource).map(new IdentityMapper<>());

		stream.addSink(pravegaWriter);

		stream.addSink(new IntSequenceExactlyOnceValidator(numElements));

		env.execute(jobName);

	}

	private static class IdentityRouter<T> implements PravegaEventRouter<T> {
		@Override
		public String getRoutingKey(T event) {
			return String.valueOf(event);
		}
	}

	public static class NotifyingMapper<T> implements MapFunction<T, T>, CheckpointListener {

		public static final AtomicReference<Runnable> TO_CALL_ON_COMPLETION = new AtomicReference<>();

		@Override
		public T map(T element) throws Exception {
			return element;
		}

		@Override
		public void notifyCheckpointComplete(long l) throws Exception {
			Runnable r = TO_CALL_ON_COMPLETION.get();
			// throttled enabled
			if (r != null) {
				TO_CALL_ON_COMPLETION.get().run();
			}
		}
	}

	public static class IdentityMapper<T> implements MapFunction<T, T> {
		@Override
		public T map(T value) throws Exception {
			return value;
		}
	}

}