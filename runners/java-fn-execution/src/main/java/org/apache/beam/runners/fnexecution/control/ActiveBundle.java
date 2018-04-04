package org.apache.beam.runners.fnexecution.control;

import java.util.Map;
import java.util.concurrent.CompletionStage;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.runners.fnexecution.state.StateDelegator;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.InboundDataClient;
import org.apache.beam.sdk.util.MoreFutures;
import org.apache.beam.sdk.util.WindowedValue;

/** An active bundle for a particular {@link BeamFnApi.ProcessBundleDescriptor}. */
public class ActiveBundle<InputT> implements AutoCloseable {
  private final String bundleId;
  private final CompletionStage<BeamFnApi.ProcessBundleResponse> response;
  private final CloseableFnDataReceiver<WindowedValue<InputT>> inputReceiver;
  private final Map<BeamFnApi.Target, InboundDataClient> outputClients;
  private final StateDelegator.Registration stateRegistration;

  ActiveBundle(
      String bundleId,
      CompletionStage<BeamFnApi.ProcessBundleResponse> response,
      CloseableFnDataReceiver<WindowedValue<InputT>> inputReceiver,
      Map<BeamFnApi.Target, InboundDataClient> outputClients,
      StateDelegator.Registration stateRegistration) {
    this.bundleId = bundleId;
    this.response = response;
    this.inputReceiver = inputReceiver;
    this.outputClients = outputClients;
    this.stateRegistration = stateRegistration;
  }

  /**
   * Returns an id used to represent this bundle.
   */
  public String getBundleId() {
    return bundleId;
  }

  /**
   * Returns a {@link FnDataReceiver receiver} which consumes input elements forwarding them
   * to the SDK.
   */
  public FnDataReceiver<WindowedValue<InputT>> getInputReceiver() {
    return inputReceiver;
  }

  /**
   * Blocks till bundle processing is finished. This is comprised of:
   * <ul>
   *   <li>closing the {@link #getInputReceiver() input receiver}.</li>
   *   <li>waiting for the SDK to say that processing the bundle is finished.</li>
   *   <li>waiting for all inbound data clients to complete</li>
   * </ul>
   *
   * <p>This method will throw an exception if bundle processing has failed.
   * {@link Throwable#getSuppressed()} will return all the reasons as to why processing has
   * failed.
   */
  @Override
  public void close() throws Exception {
    Exception exception = null;
    try {
      inputReceiver.close();
    } catch (Exception e) {
      exception = e;
    }
    try {
      // We don't have to worry about the completion stage.
      if (exception == null) {
        MoreFutures.get(response);
      } else {
        // TODO: [BEAM-3962] Handle aborting the bundle being processed.
        throw new IllegalStateException("Processing bundle failed, "
            + "TODO: [BEAM-3962] abort bundle.");
      }
    } catch (Exception e) {
      if (exception == null) {
        exception = e;
      } else {
        exception.addSuppressed(e);
      }
    }
    try {
      if (exception == null) {
        stateRegistration.deregister();
      } else {
        stateRegistration.abort();
      }
    } catch (Exception e) {
      if (exception == null) {
        exception = e;
      } else {
        exception.addSuppressed(e);
      }
    }
    for (InboundDataClient outputClient : outputClients.values()) {
      try {
        // If we failed processing this bundle, we should cancel all inbound data.
        if (exception == null) {
          outputClient.awaitCompletion();
        } else {
          outputClient.cancel();
        }
      } catch (Exception e) {
        if (exception == null) {
          exception = e;
        } else {
          exception.addSuppressed(e);
        }
      }
    }
    if (exception != null) {
      throw exception;
    }

  }
}
