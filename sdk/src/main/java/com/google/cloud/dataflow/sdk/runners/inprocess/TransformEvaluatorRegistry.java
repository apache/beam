package com.google.cloud.dataflow.sdk.runners.inprocess;

import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.Flatten.FlattenPCollectionList;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import javax.annotation.Nullable;

class TransformEvaluatorRegistry implements TransformEvaluatorFactory {
  public static TransformEvaluatorRegistry defaultRegistry() {
    @SuppressWarnings("rawtypes")
    ImmutableMap<Class<? extends PTransform>, TransformEvaluatorFactory> primitives =
        ImmutableMap.<Class<? extends PTransform>, TransformEvaluatorFactory>builder()
            .put(Read.Bounded.class, new BoundedReadEvaluatorFactory())
            .put(Read.Unbounded.class, new UnboundedReadEvaluatorFactory())
            .put(ParDo.Bound.class, new ParDoSingleEvaluatorFactory())
            .put(ParDo.BoundMulti.class, new ParDoMultiEvaluatorFactory())
            .put(
                GroupByKeyEvaluatorFactory.InProcessGroupByKeyOnly.class,
                new GroupByKeyEvaluatorFactory())
            .put(FlattenPCollectionList.class, new FlattenEvaluatorFactory())
            .put(ViewEvaluatorFactory.WriteView.class, new ViewEvaluatorFactory())
            .build();
    return new TransformEvaluatorRegistry(primitives);
  }

  @SuppressWarnings("rawtypes")
  private final Map<Class<? extends PTransform>, TransformEvaluatorFactory> factories;

  private TransformEvaluatorRegistry(
      @SuppressWarnings("rawtypes")
      Map<Class<? extends PTransform>, TransformEvaluatorFactory> factories) {
    this.factories = factories;
  }

  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application,
      @Nullable CommittedBundle<?> inputBundle,
      InProcessEvaluationContext evaluationContext)
      throws Exception {
    @SuppressWarnings("rawtypes")
    Class<? extends PTransform> applicationTransformClass = application.getTransform().getClass();
    TransformEvaluatorFactory factory = factories.get(applicationTransformClass);
    return factory.forApplication(application, inputBundle, evaluationContext);
  }
}
