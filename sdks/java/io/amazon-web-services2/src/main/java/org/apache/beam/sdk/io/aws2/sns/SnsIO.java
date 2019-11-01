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
package org.apache.beam.sdk.io.aws2.sns;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableBiConsumer;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

/**
 * {@link PTransform}s for writing to <a href="https://aws.amazon.com/sns/">SNS</a>.
 *
 * <h3>Writing to SNS</h3>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * PCollection<String> data = ...;
 *
 * data.apply(SnsIO.<String>write()
 * 		.withElementCoder(StringUtf8Coder.of())
 * 		.withPublishRequestFn(createPublishRequestFn())
 * 		.withSnsClientProvider(new BasicSnsClientProvider(awsCredentialsProvider, region));
 *
 * }</pre>
 *
 * <p>As a client, you need to provide at least the following things:
 *
 * <ul>
 *   <li>Coder for element T.
 *   <li>publishRequestFn, a function to convert your message into PublishRequest
 *   <li>SnsClientProvider, a provider to create an async client.
 * </ul>
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
final class SnsIO {
  // Write data to SNS
  static <T> Write<T> write() {
    return new AutoValue_SnsIO_Write.Builder<T>().setThrowOnError(false).build();
  }

  /** Implementation of {@link #write()}. */
  @AutoValue
  public abstract static class Write<T>
      extends PTransform<PCollection<T>, PCollection<WrappedSnsResponse<T>>> {

    @Nullable
    abstract SnsClientProvider getSnsClientProvider();

    /** Flag to indicate if io should throw an error on exception. */
    abstract boolean getThrowOnError();

    /** Coder for element T. */
    @Nullable
    abstract Coder<T> getElementCoder();

    /** SerializableFunction to create PublishRequest. */
    @Nullable
    abstract SerializableFunction<T, PublishRequest> getPublishRequestFn();

    abstract Builder<T> builder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setSnsClientProvider(SnsClientProvider clientProvider);

      abstract Builder<T> setThrowOnError(boolean throwOnError);

      abstract Builder<T> setElementCoder(Coder<T> elementCoder);

      abstract Builder<T> setPublishRequestFn(
          SerializableFunction<T, PublishRequest> publishRequestFn);

      abstract Write<T> build();
    }

    /**
     * Allows to specify a flag to indicate if io should throw an error on exception.
     *
     * @param throwOnError flag.
     */
    Write<T> withThrowOnError(boolean throwOnError) {
      return builder().setThrowOnError(throwOnError).build();
    }

    /**
     * Specify a function for converting a message into PublishRequest object.
     *
     * @param elementCoder Coder
     */
    Write<T> withElementCoder(Coder<T> elementCoder) {
      checkNotNull(elementCoder, "elementCoder cannot be null");
      return builder().setElementCoder(elementCoder).build();
    }

    /**
     * Specify a function for converting a message into PublishRequest object.
     *
     * @param publishRequestFn publishRequestFn
     */
    Write<T> withPublishRequestFn(SerializableFunction<T, PublishRequest> publishRequestFn) {
      checkNotNull(publishRequestFn, "publishRequestFn cannot be null");
      return builder().setPublishRequestFn(publishRequestFn).build();
    }

    /**
     * Allows to specify custom {@link SnsClientProvider}. {@link SnsClientProvider} creates new
     * {@link software.amazon.awssdk.services.sns.SnsAsyncClient} which is later used for writing to
     * a SNS topic.
     */
    Write<T> withSnsClientProvider(SnsClientProvider snsClientsProvider) {
      checkNotNull(snsClientsProvider, "snsClientsProvider cannot be null");
      return builder().setSnsClientProvider(snsClientsProvider).build();
    }

    /**
     * Specify credential details and region to be used to write to SNS. If you need more
     * sophisticated credential protocol, then you should look at {@link
     * Write#withSnsClientProvider(SnsClientProvider)}.
     */
    Write<T> withSnsClientProvider(AwsCredentialsProvider credentialsProvider, String region) {
      checkNotNull(credentialsProvider, "credentialsProvider cannot be null");
      checkNotNull(region, "region cannot be null");
      return withSnsClientProvider(credentialsProvider, region, null);
    }

    /**
     * Specify credential details and region to be used to write to SNS. If you need more
     * sophisticated credential protocol, then you should look at {@link
     * Write#withSnsClientProvider(SnsClientProvider)}.
     *
     * <p>The {@code serviceEndpoint} sets an alternative service host.
     */
    Write<T> withSnsClientProvider(
        AwsCredentialsProvider credentialsProvider, String region, URI serviceEndpoint) {
      checkNotNull(credentialsProvider, "credentialsProvider cannot be null");
      checkNotNull(region, "region cannot be null");
      return withSnsClientProvider(
          new BasicSnsClientProvider(credentialsProvider, region, serviceEndpoint));
    }

    @Override
    public PCollection<WrappedSnsResponse<T>> expand(PCollection<T> input) {
      checkArgument(getSnsClientProvider() != null, "withSnsClientProvider() needs to called");
      checkArgument(getPublishRequestFn() != null, "withPublishRequestFn() needs to called");
      checkArgument(getElementCoder() != null, "withElementCoder() needs to called");

      return input
          .apply(ParDo.of(new SnsAsyncWriterFn<>(this)))
          .setCoder(WrappedSnsResponseCoder.of(getElementCoder()));
    }

    private static class SnsAsyncWriterFn<T> extends DoFn<T, WrappedSnsResponse<T>> {
      private static final Logger LOG = LoggerFactory.getLogger(SnsAsyncWriterFn.class);
      private static final String ERROR_MESSAGE_DELIMITER = " ";

      private final Write<T> spec;
      private transient SnsAsyncClient client;
      private final ConcurrentLinkedQueue<SnsWriteException> failures;
      private final ConcurrentLinkedQueue<KV<WrappedSnsResponse<T>, WindowedValue<T>>> results;

      SnsAsyncWriterFn(Write<T> spec) {
        this.spec = spec;
        this.failures = new ConcurrentLinkedQueue<>();
        this.results = new ConcurrentLinkedQueue<>();
      }

      @Setup
      public void setup() {
        this.client = spec.getSnsClientProvider().getSnsAsyncClient();
      }

      @SuppressWarnings("FutureReturnValueIgnored")
      @ProcessElement
      public void processElement(ProcessContext context, BoundedWindow elementWindow)
          throws IOException {
        T element = context.element();
        Instant timestamp = context.timestamp();
        PaneInfo paneInfo = context.pane();

        WindowedValue<T> windowedValue =
            WindowedValue.of(element, timestamp, elementWindow, paneInfo);

        PublishRequest request = spec.getPublishRequestFn().apply(element);
        client.publish(request).whenComplete(whenCompleteAction(windowedValue));

        checkForFailures();
      }

      private SerializableBiConsumer<PublishResponse, ? super Throwable> whenCompleteAction(
          final WindowedValue<T> windowedValue) {

        T element = windowedValue.getValue();

        return (response, exception) -> {
          if (spec.getThrowOnError()) {
            if (exception != null) {
              failures.offer(
                  new SnsWriteException(
                      String.format("Error while publishing message: %s", element), exception));
            } else if (!response.sdkHttpResponse().isSuccessful()) {
              String customMessage =
                  response.sdkHttpResponse().statusText().isPresent()
                      ? String.format(
                          "Reason for failure: %s", response.sdkHttpResponse().statusText().get())
                      : "";

              failures.offer(
                  new SnsWriteException(
                      String.join(
                          ERROR_MESSAGE_DELIMITER,
                          String.format("Failure in publishing message: %s.", element),
                          customMessage)));
            }
          }

          WrappedSnsResponse<T> wrappedSnsResponse =
              exception != null
                  ? WrappedSnsResponse.ofError(element, exception)
                  : WrappedSnsResponse.of(element, response);

          results.offer(KV.of(wrappedSnsResponse, windowedValue));
        };
      }

      @FinishBundle
      public void finishBundle(FinishBundleContext c) throws IOException {
        checkForFailures();

        int size = results.size();
        while (size-- > 0) {
          KV<WrappedSnsResponse<T>, WindowedValue<T>> polledResponseValue = results.remove();

          WrappedSnsResponse<T> response = polledResponseValue.getKey();
          WindowedValue<T> windowedValue = polledResponseValue.getValue();
          // getWindows() returns a collection of windows but we have an element assigned only to
          // one window
          BoundedWindow window = windowedValue.getWindows().iterator().next();

          c.output(response, windowedValue.getTimestamp(), window);
        }
      }

      /** If any write has asynchronously failed, fail with a useful error. */
      private void checkForFailures() throws IOException {
        if (!spec.getThrowOnError() || failures.isEmpty()) {
          return;
        }

        int i = 0;
        List<SnsWriteException> suppressed = Lists.newArrayList();
        StringBuilder logEntry = new StringBuilder();

        for (; i < 10 && !failures.isEmpty(); ++i) {
          SnsWriteException exception = failures.remove();
          logEntry.append("\n").append(exception.getMessage());
          if (exception.getCause() != null) {
            logEntry.append(": ").append(exception.getCause().getMessage());
          }
          suppressed.add(exception);
        }

        String message =
            String.format(
                "At least %d errors occurred writing to SNS. First %d errors: %s",
                i + failures.size(), i, logEntry.toString());

        LOG.error(message);

        IOException exception = new IOException(message);
        for (SnsWriteException e : suppressed) {
          exception.addSuppressed(e);
        }
        throw exception;
      }
    }
  }

  /** Exception class for SNS write exceptions. */
  private static class SnsWriteException extends RuntimeException {
    SnsWriteException(String message) {
      super(message);
    }

    SnsWriteException(String message, Throwable error) {
      super(message, error);
    }
  }
}
