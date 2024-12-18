package org.apache.beam.fn.harness.logging;

import java.util.concurrent.CompletableFuture;

public interface LoggingClient extends AutoCloseable {

  CompletableFuture<?> terminationFuture();
}
