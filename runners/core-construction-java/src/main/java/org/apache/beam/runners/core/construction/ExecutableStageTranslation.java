package org.apache.beam.runners.core.construction;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.sdk.runners.AppliedPTransform;

/**
 * Utilities for converting {@link ExecutableStage}s to and from {@link RunnerApi} protocol buffers.
 */
public class ExecutableStageTranslation {

  /** Extracts an {@link ExecutableStagePayload} from the given transform. */
  public static ExecutableStagePayload getExecutableStagePayload(
      AppliedPTransform<?, ?, ?> appliedTransform) throws IOException {
    RunnerApi.PTransform transform =
        PTransformTranslation.toProto(appliedTransform, SdkComponents.create());
    checkArgument(ExecutableStage.URN.equals(transform.getSpec().getUrn()));
    return ExecutableStagePayload.parseFrom(transform.getSpec().getPayload());
  }

}
