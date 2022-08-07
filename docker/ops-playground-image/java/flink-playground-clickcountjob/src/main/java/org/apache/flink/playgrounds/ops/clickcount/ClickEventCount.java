package org.apache.flink.playgrounds.ops.clickcount;

import com.datastax.driver.mapping.Mapper;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ClickEventCount {

	public static final String CHECKPOINTING_OPTION = "checkpointing";
	public static final String EVENT_TIME_OPTION = "event-time";
	public static final String BACKPRESSURE_OPTION = "backpressure";
	public static final String OPERATOR_CHAINING_OPTION = "chaining";

	public static final Time WINDOW_SIZE = Time.of(15, TimeUnit.SECONDS);

	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		configureEnvironment(params, env);

		boolean inflictBackpressure = params.has(BACKPRESSURE_OPTION);


		String inputTopic = params.get("input-topic", "input");
		String brokers = params.get("bootstrap.servers", "localhost:9092");
		Properties kafkaProps = new Properties();
		kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "click-event-count");

		DataStream<ClickEvent> clicks =
				env.addSource(new FlinkKafkaConsumer<>(inputTopic, new ClickEventDeserializationSchema(), kafkaProps))
			.name("ClickEvent Source")
			.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ClickEvent>(Time.of(200, TimeUnit.MILLISECONDS)) {
				@Override
				public long extractTimestamp(final ClickEvent element) {
					return element.getTimestamp().getTime();
				}
			});

		DataStream<ClickEventPOJO> clicksWrite = clicks
				.map(x -> new ClickEventPOJO(x.getTimestamp(), x.getPage()));


		CassandraSink.addSink(clicksWrite)
				.setHost("127.0.0.1")
//				.setHost("cassandra")
				.setMapperOptions(() -> new Mapper.Option[]{Mapper.Option.saveNullFields(true)})
				.build();


		env.execute("Click Event Count");
	}

	private static void configureEnvironment(
			final ParameterTool params,
			final StreamExecutionEnvironment env) {

		boolean checkpointingEnabled = params.has(CHECKPOINTING_OPTION);
		boolean eventTimeSemantics = params.has(EVENT_TIME_OPTION);
		boolean enableChaining = params.has(OPERATOR_CHAINING_OPTION);

		if (checkpointingEnabled) {
			env.enableCheckpointing(1000);
		}

		if (eventTimeSemantics) {
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		}

		if(!enableChaining){
			//disabling Operator chaining to make it easier to follow the Job in the WebUI
			env.disableOperatorChaining();
		}
	}
}
