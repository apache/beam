/*
 * Copyright 2016-2018 Seznam.cz, a.s.
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

package cz.seznam.euphoria.beam;

import com.google.common.collect.Iterables;
import cz.seznam.euphoria.beam.io.BeamWriteSink;
import cz.seznam.euphoria.core.client.accumulators.AccumulatorProvider;
import cz.seznam.euphoria.core.client.accumulators.VoidAccumulatorProvider;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.executor.graph.DAG;
import cz.seznam.euphoria.core.util.Settings;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.PCollection;

/**
 * A {@link Flow} that can be used in Euphoria operator constructions and integrates seamlessly with
 * Beam.
 */
public class BeamFlow extends Flow {

  @SuppressFBWarnings("SE_TRANSIENT_FIELD_NOT_RESTORED")
  private final transient Map<PCollection<?>, Dataset<?>> wrapped = new HashMap<>();
  private final transient BeamExecutorContext context;
  private final transient Pipeline pipeline;
  private Duration allowedLateness = Duration.ZERO;
  private AccumulatorProvider.Factory accumulatorFactory = VoidAccumulatorProvider.getFactory();

  /**
   * Construct the {@link BeamFlow}.
   *
   * @param pipeline pipeline to wrap into this flow
   */
  private BeamFlow(String name, Pipeline pipeline) {
    super(name, new Settings());
    this.pipeline = pipeline;
    this.context =
        new BeamExecutorContext(
            DAG.empty(),
            accumulatorFactory,
            pipeline,
            getSettings(),
            org.joda.time.Duration.millis(allowedLateness.toMillis()));
  }

  /**
   * Create flow from pipeline.
   *
   * @param pipeline the pipeline to wrap into new flow
   * @return constructed flow
   */
  public static BeamFlow create(Pipeline pipeline) {
    return new BeamFlow(null, pipeline);
  }

  /**
   * Create flow from pipeline.
   *
   * @param name name of the flow
   * @param pipeline the pipeline to wrap into new flow
   * @return constructed flow
   */
  public static BeamFlow create(String name, Pipeline pipeline) {
    return new BeamFlow(name, pipeline);
  }

  @Override
  public <T> Dataset<T> createInput(DataSource<T> source) {
    Dataset<T> ret = super.createInput(source);
    PCollection<T> output = InputTranslator.doTranslate(source, context);
    context.setPCollection(ret, output);
    return ret;
  }

  /**
   * Set {@link AccumulatorProvider.Factory} to be used for accumulators.
   *
   * @param accumulatorFactory the factory to use
   * @return this
   */
  public BeamFlow setAccumulatorProvider(AccumulatorProvider.Factory accumulatorFactory) {
    this.accumulatorFactory = accumulatorFactory;
    return this;
  }

  /**
   * Convert this flow to new {@link Pipeline}. The pipeline can then be used to adding additional
   * Beam transformations.
   *
   * @param options options of the new {@link Pipeline}
   * @return the pipeline that represents transformations of this flow
   */
  public Pipeline asPipeline(PipelineOptions options) {
    return FlowTranslator.toPipeline(
        this,
        accumulatorFactory,
        options,
        getSettings(),
        org.joda.time.Duration.millis(allowedLateness.toMillis()));
  }

  /**
   * Wrap given {@link PCollection} as {@link Dataset} into this flow.
   *
   * @param <T> type parameter
   * @param coll the collection
   * @return wrapped {@link Dataset}
   */
  @SuppressWarnings("unchecked")
  public <T> Dataset<T> wrapped(PCollection<T> coll) {
    return (Dataset<T>)
        wrapped.compute(
            coll,
            (key, current) -> {
              if (current == null) {
                return newDataset(coll);
              }
              return current;
            });
  }

  /**
   * Return raw Beam's {@link PCollection} represented by given {@link Dataset}.
   *
   * @param <T> type parameter
   * @param dataset dataset to return Beam's representation for
   * @return {@link PCollection} represented by given dataset
   */
  public <T> PCollection<T> unwrapped(Dataset<T> dataset) {
    Operator<?, T> producer = dataset.getProducer();
    if (producer != null) {
      dataset = producer.output();
    }
    Dataset<T> search = dataset;
    return context
        .getPCollection(search)
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "Dataset " + search + " was not created by this flow!"));
  }

  private <T> Dataset<T> newDataset(PCollection<T> coll) {
    Operator<?, T> wrap = new WrappedPCollectionOperator<>(this, coll);
    add(wrap);
    return wrap.output();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <InputT, OutputT, T extends Operator<InputT, OutputT>> T add(T operator) {
    T ret = super.add(operator);
    List<Operator<?, ?>> inputOperators =
        operator
            .listInputs()
            .stream()
            .map(d -> (Operator<?, ?>) new WrappedPCollectionOperator(this, unwrapped(d), d))
            .collect(Collectors.toList());
    final DAG<Operator<?, ?>> dag;
    if (inputOperators.isEmpty()) {
      dag = DAG.of(operator);
    } else {
      dag = DAG.of(inputOperators);
      dag.add(operator, inputOperators);
    }
    DAG<Operator<?, ?>> unfolded = FlowTranslator.unfold(dag);
    context.setTranslationDAG(unfolded);
    FlowTranslator.updateContextBy(unfolded, context);
    // register the output of the sub-dag as output of the original operator
    Dataset<OutputT> output = operator.output();
    Dataset<OutputT> dagOutput = (Dataset) Iterables.getOnlyElement(unfolded.getLeafs()).get()
        .output();
    if (output != dagOutput) {
      context.setPCollection(output, unwrapped(dagOutput));
    }
    return ret;
  }

  @Override
  public <T> void onPersisted(Dataset<T> dataset) {
    if (pipeline != null) {
      PCollection<T> coll =
          context
              .getPCollection(dataset)
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "Persisting dataset not created by this flow! Fix code!"));
      coll.apply(BeamWriteSink.wrap(dataset.getOutputSink()));
    }
  }

  /**
   * Specify allowed lateness for all reducing operations.
   *
   * @param allowedLateness the allowed lateness
   * @return this
   */
  public BeamFlow withAllowedLateness(Duration allowedLateness) {
    this.allowedLateness = allowedLateness;
    return this;
  }

  /**
   * Retrieve {@link Pipeline} associated with this {@link BeamFlow}.
   *
   * @return associated pipeline
   * @throws NullPointerException when the flow has no associated pipeline. Note that the flow has
   * associated pipeline if and only if it was created by {@link #create(Pipeline)}.
   */
  public Pipeline getPipeline() {
    return Objects.requireNonNull(pipeline);
  }

  /**
   * @return {@code true} if this flow already has associated {@link Pipeline}.
   */
  boolean hasPipeline() {
    return pipeline != null;
  }
}
