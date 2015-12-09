/*
 * Copyright 2015 Data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dataartisans.flink.dataflow.examples.streaming;

import com.dataartisans.flink.dataflow.FlinkPipelineRunner;
import com.dataartisans.flink.dataflow.translation.wrappers.streaming.io.UnboundedFlinkSource;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.transforms.windowing.*;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.joda.time.Duration;

import java.util.Properties;

public class KafkaWindowedWordCountExample {

	static final String KAFKA_TOPIC = "test";  // Default kafka topic to read from
	static final String KAFKA_BROKER = "localhost:9092";  // Default kafka broker to contact
	static final String GROUP_ID = "myGroup";  // Default groupId
	static final String ZOOKEEPER = "localhost:2181";  // Default zookeeper to connect to for Kafka

	public static class ExtractWordsFn extends DoFn<String, String> {
		private final Aggregator<Long, Long> emptyLines =
				createAggregator("emptyLines", new Sum.SumLongFn());

		@Override
		public void processElement(ProcessContext c) {
			if (c.element().trim().isEmpty()) {
				emptyLines.addValue(1L);
			}

			// Split the line into words.
			String[] words = c.element().split("[^a-zA-Z']+");

			// Output each word encountered into the output PCollection.
			for (String word : words) {
				if (!word.isEmpty()) {
					c.output(word);
				}
			}
		}
	}

	public static class FormatAsStringFn extends DoFn<KV<String, Long>, String> {
		@Override
		public void processElement(ProcessContext c) {
			String row = c.element().getKey() + " - " + c.element().getValue() + " @ " + c.timestamp().toString();
			System.out.println(row);
			c.output(row);
		}
	}

	public static interface KafkaStreamingWordCountOptions extends WindowedWordCount.StreamingWordCountOptions {
		@Description("The Kafka topic to read from")
		@Default.String(KAFKA_TOPIC)
		String getKafkaTopic();

		void setKafkaTopic(String value);

		@Description("The Kafka Broker to read from")
		@Default.String(KAFKA_BROKER)
		String getBroker();

		void setBroker(String value);

		@Description("The Zookeeper server to connect to")
		@Default.String(ZOOKEEPER)
		String getZookeeper();

		void setZookeeper(String value);

		@Description("The groupId")
		@Default.String(GROUP_ID)
		String getGroup();

		void setGroup(String value);

	}

	public static void main(String[] args) {
		PipelineOptionsFactory.register(KafkaStreamingWordCountOptions.class);
		KafkaStreamingWordCountOptions options = PipelineOptionsFactory.fromArgs(args).as(KafkaStreamingWordCountOptions.class);
		options.setJobName("KafkaExample");
		options.setStreaming(true);
		options.setRunner(FlinkPipelineRunner.class);

		System.out.println(options.getKafkaTopic() +" "+ options.getZookeeper() +" "+ options.getBroker() +" "+ options.getGroup() );
		Pipeline pipeline = Pipeline.create(options);

		Properties p = new Properties();
		p.setProperty("zookeeper.connect", options.getZookeeper());
		p.setProperty("bootstrap.servers", options.getBroker());
		p.setProperty("group.id", options.getGroup());

		// this is the Flink consumer that reads the input to
		// the program from a kafka topic.
		FlinkKafkaConsumer082 kafkaConsumer = new FlinkKafkaConsumer082<>(
				options.getKafkaTopic(),
				new SimpleStringSchema(), p);

		PCollection<String> words = pipeline
				.apply(Read.from(new UnboundedFlinkSource<String, UnboundedSource.CheckpointMark>(options, kafkaConsumer)).named("StreamingWordCount"))
				.apply(ParDo.of(new ExtractWordsFn()))
				.apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(options.getWindowSize())))
						.triggering(AfterWatermark.pastEndOfWindow()).withAllowedLateness(Duration.ZERO)
						.discardingFiredPanes());

		PCollection<KV<String, Long>> wordCounts =
				words.apply(Count.<String>perElement());

		wordCounts.apply(ParDo.of(new FormatAsStringFn()))
				.apply(TextIO.Write.to("./outputKafka.txt"));

		pipeline.run();
	}
}
