package org.apache.beam.runners.fnexecution.control;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.runners.fnexecution.data.FnDataService;
import org.apache.beam.runners.fnexecution.data.RemoteInputDestination;
import org.apache.beam.runners.fnexecution.state.StateDelegator;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.InboundDataClient;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.util.WindowedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A processor capable of creating bundles for some registered
 * {@link BeamFnApi.ProcessBundleDescriptor}.
 */
public class BundleProcessor<T> {
  private static final Logger LOG = LoggerFactory.getLogger(BundleProcessor.class);

  private final BeamFnApi.ProcessBundleDescriptor processBundleDescriptor;
  private final CompletionStage<BeamFnApi.RegisterResponse> registrationFuture;
  private final RemoteInputDestination<WindowedValue<T>> remoteInput;
  private final StateDelegator stateDelegator;
  private InstructionRequestHandler fnApiControlClient;
  private FnDataService fnApiDataService;
  private SdkHarnessClient.IdGenerator idGenerator;

  BundleProcessor(
      InstructionRequestHandler fnApiControlClient,
      FnDataService fnApiDataService,
      SdkHarnessClient.IdGenerator idGenerator,
      BeamFnApi.ProcessBundleDescriptor processBundleDescriptor,
      CompletionStage<BeamFnApi.RegisterResponse> registrationFuture,
      RemoteInputDestination<WindowedValue<T>> remoteInput,
      StateDelegator stateDelegator) {
    this.processBundleDescriptor = processBundleDescriptor;
    this.registrationFuture = registrationFuture;
    this.remoteInput = remoteInput;
    this.stateDelegator = stateDelegator;
    this.fnApiControlClient = fnApiControlClient;
    this.fnApiDataService = fnApiDataService;
    this.idGenerator = idGenerator;
  }

  public CompletionStage<BeamFnApi.RegisterResponse> getRegistrationFuture() {
    return registrationFuture;
  }

  /**
   * Start a new bundle for the given {@link BeamFnApi.ProcessBundleDescriptor} identifier.
   *
   * <p>The input channels for the returned {@link ActiveBundle} are derived from the instructions
   * in the {@link BeamFnApi.ProcessBundleDescriptor}.
   *
   * <p>NOTE: It is important to {@link ActiveBundle#close()} each bundle after all elements are
   * emitted.
   *
   * <pre>{@code
   * try (ActiveBundle<InputT> bundle = SdkHarnessClient.newBundle(...)) {
   *   FnDataReceiver<InputT> inputReceiver = bundle.getInputReceiver();
   *   // send all elements ...
   * }
   * }</pre>
   */
  public ActiveBundle<T> newBundle(
      Map<BeamFnApi.Target, RemoteOutputReceiver<?>> outputReceivers) {
    return newBundle(outputReceivers, request -> {
      throw new UnsupportedOperationException(String.format(
          "The %s does not have a registered state handler.",
          ActiveBundle.class.getSimpleName()));
    });
  }

  /**
   * Start a new bundle for the given {@link BeamFnApi.ProcessBundleDescriptor} identifier.
   *
   * <p>The input channels for the returned {@link ActiveBundle} are derived from the instructions
   * in the {@link BeamFnApi.ProcessBundleDescriptor}.
   *
   * <p>NOTE: It is important to {@link ActiveBundle#close()} each bundle after all elements are
   * emitted.
   *
   * <pre>{@code
   * try (ActiveBundle<InputT> bundle = SdkHarnessClient.newBundle(...)) {
   *   FnDataReceiver<InputT> inputReceiver = bundle.getInputReceiver();
   *   // send all elements ...
   * }
   * }</pre>
   */
  public ActiveBundle<T> newBundle(
      Map<BeamFnApi.Target, RemoteOutputReceiver<?>> outputReceivers,
      StateRequestHandler stateRequestHandler) {
    String bundleId = idGenerator.getId();

    final CompletionStage<BeamFnApi.InstructionResponse> genericResponse =
        fnApiControlClient.handle(
            BeamFnApi.InstructionRequest.newBuilder()
                .setInstructionId(bundleId)
                .setProcessBundle(
                    BeamFnApi.ProcessBundleRequest.newBuilder()
                        .setProcessBundleDescriptorReference(processBundleDescriptor.getId()))
                .build());
    LOG.debug(
        "Sent {} with ID {} for {} with ID {}",
        BeamFnApi.ProcessBundleRequest.class.getSimpleName(),
        bundleId,
        BeamFnApi.ProcessBundleDescriptor.class.getSimpleName(),
        processBundleDescriptor.getId());

    CompletionStage<BeamFnApi.ProcessBundleResponse> specificResponse =
        genericResponse.thenApply(BeamFnApi.InstructionResponse::getProcessBundle);
    Map<BeamFnApi.Target, InboundDataClient> outputClients = new HashMap<>();
    for (Map.Entry<BeamFnApi.Target, RemoteOutputReceiver<?>> targetReceiver
        : outputReceivers.entrySet()) {
      InboundDataClient outputClient =
          attachReceiver(
              bundleId,
              targetReceiver.getKey(),
              (RemoteOutputReceiver) targetReceiver.getValue());
      outputClients.put(targetReceiver.getKey(), outputClient);
    }

    CloseableFnDataReceiver<WindowedValue<T>> dataReceiver =
        fnApiDataService.send(
            LogicalEndpoint.of(bundleId, remoteInput.getTarget()), remoteInput.getCoder());

    return new ActiveBundle(
        bundleId,
        specificResponse,
        dataReceiver,
        outputClients,
        stateDelegator.registerForProcessBundleInstructionId(bundleId, stateRequestHandler));
  }

  private <OutputT> InboundDataClient attachReceiver(
      String bundleId,
      BeamFnApi.Target target,
      RemoteOutputReceiver<WindowedValue<OutputT>> receiver) {
    return fnApiDataService.receive(
        LogicalEndpoint.of(bundleId, target), receiver.getCoder(), receiver.getReceiver());
  }

  BeamFnApi.ProcessBundleDescriptor getProcessBundleDescriptor() {
    return processBundleDescriptor;
  }
}
