/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.rabbitmq;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

import com.google.auto.value.AutoValue;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.LongString;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;

/**
 * A IO to publish or consume messages with a RabbitMQ broker.
 *
 * <h3>Consuming messages from RabbitMQ server</h3>
 *
 * <p>
 * {@link RabbitMqIO} {@link Read} returns an unbounded {@link PCollection}
 * containing RabbitMQ messages body (as {@code byte[]}) wrapped as
 * {@link RabbitMqMessage}.
 *
 * <p>
 * To configure a RabbitMQ source, you have to provide a RabbitMQ {@code URI} to
 * connect to a RabbitMQ broker. The following example illustrates various
 * options for configuring the source, reading from the queue:
 *
 * <pre>
 * {@code
 * PCollection<RabbitMqMessage> messages = pipeline.apply(
 *   RabbitMqIO.read().withUri("amqp://user:password@localhost:5672").withQueue("QUEUE"))
 *
 * }
 * </pre>
 *
 * <p>
 * It's also possible to read from an exchange (providing the exchange type and
 * routing key) instead of directly from a queue:
 *
 * <pre>
 * {
 * 	&#64;code
 * 	PCollection<RabbitMqMessage> messages = pipeline.apply(RabbitMqIO.read()
 * 			.withUri("amqp://user:password@localhost:5672").withExchange("EXCHANGE", "fanout", "QUEUE"));
 * }
 * </pre>
 *
 * <h3>Publishing messages to RabbitMQ server</h3>
 *
 * <p>
 * {@link RabbitMqIO} {@link Write} can send {@link RabbitMqMessage} to a
 * RabbitMQ server queue or exchange.
 *
 * <p>
 * As for the {@link Read}, the {@link Write} is configured with a RabbitMQ URI.
 *
 * <p>
 * For instance, you can write to an exchange (providing the exchange type):
 *
 * <pre>
 * {@code
 * pipeline
 *   .apply(...) // provide PCollection<RabbitMqMessage>
 *   .apply(RabbitMqIO.write().withUri("amqp://user:password@localhost:5672").withExchange("EXCHANGE", "fanout"));
 * }
 * </pre>
 *
 * <p>
 * For instance, you can write to a queue:
 *
 * <pre>
 * {@code
 * pipeline
 *   .apply(...) // provide PCollection<RabbitMqMessage>
 *   .apply(RabbitMqIO.write().withUri("amqp://user:password@localhost:5672").withQueue("QUEUE"));
 *
 * }
 * </pre>
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class RabbitMqIO {
	public static Read read() {
		return new AutoValue_RabbitMqIO_Read.Builder().setQueueDeclare(false).setMaxReadTime(null)
				.setMaxNumRecords(Long.MAX_VALUE).setUseCorrelationId(false).build();
	}

	public static Write write() {
		return new AutoValue_RabbitMqIO_Write.Builder().setExchangeDeclare(false).setQueueDeclare(false).build();
	}

	private RabbitMqIO() {
	}

	private static class ConnectionHandler {

		private final ConnectionFactory connectionFactory;
		private Connection connection;
		private Channel channel;

		public ConnectionHandler(String uri)
				throws URISyntaxException, NoSuchAlgorithmException, KeyManagementException {
			connectionFactory = new ConnectionFactory();
			connectionFactory.setUri(uri);
			connectionFactory.setAutomaticRecoveryEnabled(true);
			connectionFactory.setConnectionTimeout(60000);
			connectionFactory.setNetworkRecoveryInterval(5000);
			connectionFactory.setRequestedHeartbeat(60);
			connectionFactory.setTopologyRecoveryEnabled(true);
			connectionFactory.setRequestedChannelMax(0);
			connectionFactory.setRequestedFrameMax(0);
		}

		public void start() throws TimeoutException, IOException {
			connection = connectionFactory.newConnection();
			channel = connection.createChannel();
			if (channel == null) {
				throw new IOException("No RabbitMQ channel available");
			}
		}

		public Channel getChannel() {
			return this.channel;
		}

		public void stop() throws IOException {
			if (channel != null) {
				try {
					channel.close();
				} catch (Exception e) {
					// ignore
				}
			}
			if (connection != null) {
				connection.close();
			}
		}
	}

	/** A {@link PTransform} to consume messages from RabbitMQ server. */
	@AutoValue
	public abstract static class Read extends PTransform<PBegin, PCollection<RabbitMqMessage>> {
		@Nullable
		abstract String uri();

		@Nullable
		abstract String queue();

		abstract boolean queueDeclare();

		@Nullable
		abstract String exchange();

		@Nullable
		abstract String exchangeType();

		@Nullable
		abstract String routingKey();

		abstract boolean useCorrelationId();

		abstract long maxNumRecords();

		@Nullable
		abstract Duration maxReadTime();

		abstract Builder builder();

		@AutoValue.Builder
		abstract static class Builder {
			abstract Builder setUri(String uri);

			abstract Builder setQueue(String queue);

			abstract Builder setQueueDeclare(boolean queueDeclare);

			abstract Builder setExchange(String exchange);

			abstract Builder setExchangeType(String exchangeType);

			abstract Builder setRoutingKey(String routingKey);

			abstract Builder setUseCorrelationId(boolean useCorrelationId);

			abstract Builder setMaxNumRecords(long maxNumRecords);

			abstract Builder setMaxReadTime(Duration maxReadTime);

			abstract Read build();
		}

		public Read withUri(String uri) {
			checkArgument(uri != null, "uri can not be null");
			return builder().setUri(uri).build();
		}

		/**
		 * If you want to directly consume messages from a specific queue, you just have
		 * to specify the queue name. Optionally, you can declare the queue using
		 * {@link RabbitMqIO.Read#withQueueDeclare(boolean)}.
		 */
		public Read withQueue(String queue) {
			checkArgument(queue != null, "queue can not be null");
			return builder().setQueue(queue).build();
		}

		/**
		 * You can "force" the declaration of a queue on the RabbitMQ broker. Exchanges
		 * and queues are the high-level building blocks of AMQP. These must be
		 * "declared" before they can be used. Declaring either type of object simply
		 * ensures that one of that name exists, creating it if necessary.
		 *
		 * @param queueDeclare If {@code true}, {@link RabbitMqIO} will declare the
		 *                     queue. If another application declare the queue, it's not
		 *                     required.
		 */
		public Read withQueueDeclare(boolean queueDeclare) {
			return builder().setQueueDeclare(queueDeclare).build();
		}

		/**
		 * Instead of consuming messages on a specific queue, you can consume message
		 * from a given exchange. Then you specify the exchange name, type and
		 * optionally routing key where you want to consume messages.
		 */
		public Read withExchange(String name, String type, String routingKey) {
			checkArgument(name != null, "name can not be null");
			checkArgument(type != null, "type can not be null");
			return builder().setExchange(name).setExchangeType(type).setRoutingKey(routingKey).build();
		}

		/**
		 * Define the max number of records received by the {@link Read}. When this max
		 * number of records is lower than {@code Long.MAX_VALUE}, the {@link Read} will
		 * provide a bounded {@link PCollection}.
		 */
		public Read withMaxNumRecords(long maxNumRecords) {
			checkArgument(maxReadTime() == null, "maxNumRecord and maxReadTime are exclusive");
			return builder().setMaxNumRecords(maxNumRecords).build();
		}

		/**
		 * Define the max read time (duration) while the {@link Read} will receive
		 * messages. When this max read time is not null, the {@link Read} will provide
		 * a bounded {@link PCollection}.
		 */
		public Read withMaxReadTime(Duration maxReadTime) {
			checkArgument(maxNumRecords() == Long.MAX_VALUE, "maxNumRecord and maxReadTime are exclusive");
			return builder().setMaxReadTime(maxReadTime).build();
		}

		@Override
		public PCollection<RabbitMqMessage> expand(PBegin input) {
			org.apache.beam.sdk.io.Read.Unbounded<RabbitMqMessage> unbounded = org.apache.beam.sdk.io.Read
					.from(new RabbitMQSource(this));

			PTransform<PBegin, PCollection<RabbitMqMessage>> transform = unbounded;

			if (maxNumRecords() < Long.MAX_VALUE || maxReadTime() != null) {
				transform = unbounded.withMaxReadTime(maxReadTime()).withMaxNumRecords(maxNumRecords());
			}

			return input.getPipeline().apply(transform);
		}
	}

	static class RabbitMQSource extends UnboundedSource<RabbitMqMessage, RabbitMQCheckpointMark> {
		final Read spec;

		RabbitMQSource(Read spec) {
			this.spec = spec;
		}

		@Override
		public Coder<RabbitMqMessage> getOutputCoder() {
			return SerializableCoder.of(RabbitMqMessage.class);
		}

		@Override
		public List<RabbitMQSource> split(int desiredNumSplits, PipelineOptions options) {
			// RabbitMQ uses queue, so, we can have several concurrent consumers as source
			List<RabbitMQSource> sources = new ArrayList<>();
			for (int i = 0; i < desiredNumSplits; i++) {
				sources.add(this);
			}
			return sources;
		}

		@Override
		public UnboundedReader<RabbitMqMessage> createReader(PipelineOptions options,
				RabbitMQCheckpointMark checkpointMark) throws IOException {
			return new UnboundedRabbitMqReader(this, checkpointMark);
		}

		@Override
		public Coder<RabbitMQCheckpointMark> getCheckpointMarkCoder() {
			return SerializableCoder.of(RabbitMQCheckpointMark.class);
		}

		@Override
		public boolean requiresDeduping() {
			return spec.useCorrelationId();
		}
	}

	private static class RabbitMQCheckpointMark implements UnboundedSource.CheckpointMark, Serializable {
		transient Channel channel;
		Instant oldestTimestamp = Instant.now();
		final List<Long> sessionIds = new ArrayList<>();

		@Override
		public void finalizeCheckpoint() throws IOException {
			for (Long sessionId : sessionIds) {
				try {
					channel.basicAck(sessionId, false);
				} catch (IOException e) {
					channel.basicNack(sessionId, false, true);
				}
			}
			channel.txCommit();
			oldestTimestamp = Instant.now();
			sessionIds.clear();
		}
	}

	private static class UnboundedRabbitMqReader extends UnboundedSource.UnboundedReader<RabbitMqMessage> {
		private final RabbitMQSource source;

		private RabbitMqMessage current;
		private byte[] currentRecordId;
		private ConnectionHandler connectionHandler;
		private QueueingConsumer consumer;
		private Instant currentTimestamp;
		private final RabbitMQCheckpointMark checkpointMark;

		UnboundedRabbitMqReader(RabbitMQSource source, RabbitMQCheckpointMark checkpointMark) throws IOException {
			this.source = source;
			this.current = null;
			this.checkpointMark = checkpointMark != null ? checkpointMark : new RabbitMQCheckpointMark();
			try {
				connectionHandler = new ConnectionHandler(source.spec.uri());
			} catch (Exception e) {
				throw new IOException(e);
			}
		}

		@Override
		public Instant getWatermark() {
			return checkpointMark.oldestTimestamp;
		}

		@Override
		public UnboundedSource.CheckpointMark getCheckpointMark() {
			return checkpointMark;
		}

		@Override
		public RabbitMQSource getCurrentSource() {
			return source;
		}

		@Override
		public byte[] getCurrentRecordId() {
			if (current == null) {
				throw new NoSuchElementException();
			}
			if (currentRecordId != null) {
				return currentRecordId;
			} else {
				return "".getBytes(StandardCharsets.UTF_8);
			}
		}

		@Override
		public Instant getCurrentTimestamp() {
			if (currentTimestamp == null) {
				throw new NoSuchElementException();
			}
			return currentTimestamp;
		}

		@Override
		public RabbitMqMessage getCurrent() {
			if (current == null) {
				throw new NoSuchElementException();
			}
			return current;
		}

		@Override
		public boolean start() throws IOException {
			try {
				ConnectionHandler connectionHandler = new ConnectionHandler(source.spec.uri());
				connectionHandler.start();

				Channel channel = connectionHandler.getChannel();

				String queueName = source.spec.queue();
				if (source.spec.queueDeclare()) {
					// declare the queue (if not done by another application)
					// channel.queueDeclare(queueName, durable, exclusive, autoDelete, arguments);
					channel.queueDeclare(queueName, false, false, false, null);
				}
				if (source.spec.exchange() != null) {
					channel.exchangeDeclare(source.spec.exchange(), source.spec.exchangeType());
					if (queueName == null) {
						queueName = channel.queueDeclare().getQueue();
					}
					channel.queueBind(queueName, source.spec.exchange(), source.spec.routingKey());
				}
				checkpointMark.channel = channel;
				consumer = new QueueingConsumer(channel);
				channel.txSelect();
				// we consume message without autoAck (we want to do the ack ourselves)
				channel.setDefaultConsumer(consumer);
				channel.basicConsume(queueName, false, consumer);
			} catch (Exception e) {
				throw new IOException(e);
			}
			return advance();
		}

		@Override
		public boolean advance() throws IOException {
			try {
				QueueingConsumer.Delivery delivery = consumer.nextDelivery(1000);
				if (delivery == null) {
					return false;
				}
				delivery = serializableDeliveryOf(delivery);
				if (source.spec.useCorrelationId()) {
					String correlationId = delivery.getProperties().getCorrelationId();
					if (correlationId == null) {
						throw new IOException("RabbitMqIO.Read uses message correlation ID, but received "
								+ "message has a null correlation ID");
					}
					currentRecordId = correlationId.getBytes(StandardCharsets.UTF_8);
				}
				long deliveryTag = delivery.getEnvelope().getDeliveryTag();
				checkpointMark.sessionIds.add(deliveryTag);

				current = new RabbitMqMessage(source.spec.routingKey(), delivery);
				currentTimestamp = new Instant(delivery.getProperties().getTimestamp());
				if (currentTimestamp.isBefore(checkpointMark.oldestTimestamp)) {
					checkpointMark.oldestTimestamp = currentTimestamp;
				}
			} catch (Exception e) {
				throw new IOException(e);
			}
			return true;
		}

		/**
		 * ake delivery serializable by cloning all non-serializable values into
		 * serializable ones. If it is not possible, initial delivery is returned and
		 * error message is logged
		 * 
		 * @param processed
		 * @return
		 */
		private Delivery serializableDeliveryOf(Delivery processed) {
			// All content of envelope is serializable, so no problem there
			Envelope envelope = processed.getEnvelope();
			// in basicproperties, there may be LongString, which are *not* serializable
			BasicProperties properties = processed.getProperties();
			BasicProperties nextProperties = new BasicProperties.Builder().appId(properties.getAppId())
					.clusterId(properties.getClusterId()).contentEncoding(properties.getContentEncoding())
					.contentType(properties.getContentType()).correlationId(properties.getCorrelationId())
					.deliveryMode(properties.getDeliveryMode()).expiration(properties.getExpiration())
					.headers(serializableHeaders(properties.getHeaders()))
					.messageId(properties.getMessageId())
					.priority(properties.getPriority())
					.replyTo(properties.getReplyTo())
					.timestamp(properties.getTimestamp())
					.type(properties.getType())
					.userId(properties.getUserId())
					.build();
			return new Delivery(envelope, nextProperties, processed.getBody());
		}

		private Map<String, Object> serializableHeaders(Map<String, Object> headers) {
			Map<String, Object> returned = new HashMap<>();
			for (Map.Entry<String, Object> h : headers.entrySet()) {
				Object value = h.getValue();
				if (!(value instanceof Serializable)) {
					try {
						if (value instanceof LongString) {
							LongString longString = (LongString) value;
							byte[] bytes = longString.getBytes();
							String s = new String(bytes, "UTF-8");
							value = s;
						} else {
							throw new RuntimeException(String.format("no transformation defined for %s", value));
						}
					} catch (Throwable t) {
						throw new UnsupportedOperationException(String.format(
								"can't make unserializable value %s a serializable value (which is mandatory for Apache Beam dataflow implementation)",
								value), t);
					}
				}
				returned.put(h.getKey(), value);
			}
			return returned;
		}

		@Override
		public void close() throws IOException {
			if (connectionHandler != null) {
				connectionHandler.stop();
			}
		}
	}

	/** A {@link PTransform} to publish messages to a RabbitMQ server. */
	@AutoValue
	public abstract static class Write extends PTransform<PCollection<RabbitMqMessage>, PCollection<?>> {

		@Nullable
		abstract String uri();

		@Nullable
		abstract String exchange();

		@Nullable
		abstract String exchangeType();

		abstract boolean exchangeDeclare();

		@Nullable
		abstract String queue();

		abstract boolean queueDeclare();

		abstract Builder builder();

		@AutoValue.Builder
		abstract static class Builder {
			abstract Builder setUri(String uri);

			abstract Builder setExchange(String exchange);

			abstract Builder setExchangeType(String exchangeType);

			abstract Builder setExchangeDeclare(boolean exchangeDeclare);

			abstract Builder setQueue(String queue);

			abstract Builder setQueueDeclare(boolean queueDeclare);

			abstract Write build();
		}

		public Write withUri(String uri) {
			checkArgument(uri != null, "uri can not be null");
			return builder().setUri(uri).build();
		}

		/**
		 * Defines the exchange where the messages will be sent. The exchange has to be
		 * declared. It can be done by another application or by {@link RabbitMqIO} if
		 * you define {@code true} for
		 * {@link RabbitMqIO.Write#withExchangeDeclare(boolean)}.
		 */
		public Write withExchange(String exchange, String exchangeType) {
			checkArgument(exchange != null, "exchange can not be null");
			checkArgument(exchangeType != null, "exchangeType can not be null");
			return builder().setExchange(exchange).setExchangeType(exchangeType).build();
		}

		/**
		 * If the exchange is not declared by another application, {@link RabbitMqIO}
		 * can declare the exchange itself.
		 *
		 * @param exchangeDeclare {@code true} to declare the exchange, {@code false}
		 *                        else.
		 */
		public Write withExchangeDeclare(boolean exchangeDeclare) {
			return builder().setExchangeDeclare(exchangeDeclare).build();
		}

		/**
		 * Defines the queue where the messages will be sent. The queue has to be
		 * declared. It can be done by another application or by {@link RabbitMqIO} if
		 * you set {@link Write#withQueueDeclare} to {@code true}.
		 */
		public Write withQueue(String queue) {
			checkArgument(queue != null, "queue can not be null");
			return builder().setQueue(queue).build();
		}

		/**
		 * If the queue is not declared by another application, {@link RabbitMqIO} can
		 * declare the queue itself.
		 *
		 * @param queueDeclare {@code true} to declare the queue, {@code false} else.
		 */
		public Write withQueueDeclare(boolean queueDeclare) {
			return builder().setQueueDeclare(queueDeclare).build();
		}

		@Override
		public PCollection<?> expand(PCollection<RabbitMqMessage> input) {
			checkArgument(exchange() != null || queue() != null, "Either exchange or queue has to be specified");
			if (exchange() != null) {
				checkArgument(queue() == null, "Queue can't be set in the same time as exchange");
			}
			if (queue() != null) {
				checkArgument(exchange() == null, "Exchange can't be set in the same time as queue");
			}
			if (queueDeclare()) {
				checkArgument(queue() != null, "Queue is required for the queue declare");
			}
			if (exchangeDeclare()) {
				checkArgument(exchange() != null, "Exchange is required for the exchange declare");
			}
			return input.apply(ParDo.of(new WriteFn(this)));
		}

		private static class WriteFn extends DoFn<RabbitMqMessage, Void> {
			private final Write spec;

			private transient ConnectionHandler connectionHandler;

			WriteFn(Write spec) {
				this.spec = spec;
			}

			@Setup
			public void setup() throws Exception {
				connectionHandler = new ConnectionHandler(spec.uri());
				connectionHandler.start();

				Channel channel = connectionHandler.getChannel();

				if (spec.exchange() != null && spec.exchangeDeclare()) {
					channel.exchangeDeclare(spec.exchange(), spec.exchangeType());
				}
				if (spec.queue() != null && spec.queueDeclare()) {
					channel.queueDeclare(spec.queue(), true, false, false, null);
				}
			}

			@ProcessElement
			public void processElement(ProcessContext c) throws IOException {
				RabbitMqMessage message = c.element();
				Channel channel = connectionHandler.getChannel();

				if (spec.exchange() != null) {
					channel.basicPublish(spec.exchange(), message.getRoutingKey(), message.createProperties(),
							message.getBody());
				}
				if (spec.queue() != null) {
					channel.basicPublish("", spec.queue(), MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBody());
				}
			}

			@Teardown
			public void teardown() throws Exception {
				if (connectionHandler != null) {
					connectionHandler.stop();
				}
			}
		}
	}
}
