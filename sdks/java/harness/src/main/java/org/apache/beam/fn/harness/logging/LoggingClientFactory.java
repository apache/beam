package org.apache.beam.fn.harness.logging;

import io.grpc.ManagedChannel;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.SdkHarnessOptions;

/**
 * A factory for {@link LoggingClient}s. Provides {@link BeamFnLoggingClient} if the logging service
 * is enabled, otherwise provides a no-op client.
 */
public class LoggingClientFactory {

  private LoggingClientFactory() {}

  /**
   * A factory for {@link LoggingClient}s. Provides {@link BeamFnLoggingClient} if the logging
   * service is enabled, otherwise provides a no-op client.
   */
  public static LoggingClient createAndStart(
      PipelineOptions options,
      Endpoints.ApiServiceDescriptor apiServiceDescriptor,
      Function<Endpoints.ApiServiceDescriptor, ManagedChannel> channelFactory) {
    if (options.as(SdkHarnessOptions.class).getEnableLoggingService()) {
      return BeamFnLoggingClient.createAndStart(options, apiServiceDescriptor, channelFactory);
    } else {
      return new NoOpLoggingClient();
    }
  }

  static final class NoOpLoggingClient implements LoggingClient {
    @Override
    public CompletableFuture<?> terminationFuture() {
      return CompletableFuture.completedFuture(new Object());
    }

    @Override
    public void close() throws Exception {}
  }
}
