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
package org.apache.beam.sdk.extensions.euphoria.core.client.operator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.Recommended;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.StateComplexity;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.stability.Experimental;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Window;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Windowing;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.BinaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Builders;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.OptionalMethodBuilder;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.StateAwareWindowWiseOperator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.OutputHint;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ListStorage;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ListStorageDescriptor;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.State;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.StateContext;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.StorageProvider;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.windowing.WindowingDesc;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeUtils;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Either;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.extensions.euphoria.core.executor.graph.DAG;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * Inner join of two datasets by given key producing single new dataset.
 *
 * <p>When joining two streams, the join has to specify {@link Windowing} which groups elements from
 * streams into {@link Window}s. The join operation is performed within same windows produced on
 * left and right side of input {@link Dataset}s.
 *
 * <h3>Builders:</h3>
 *
 * <ol>
 *   <li>{@code [named] ..................} give name to the operator [optional]
 *   <li>{@code of .......................} left and right input dataset
 *   <li>{@code by .......................} {@link UnaryFunction}s transforming left and right
 *       elements into keys
 *   <li>{@code using ....................} {@link BinaryFunctor} receiving left and right element
 *       from joined window
 *   <li>{@code [windowBy] ...............} windowing (see {@link WindowFn}), default is no
 *       windowing
 *   <li>{@code [triggeredBy] ............} defines windowing trigger, follows [windowBy] if called
 *   <li>{@code [accumulationMode] .......} windowing accumulation mode, follows [triggeredBy]
 *   <li>{@code (output | outputValues) ..} build output dataset
 * </ol>
 */
@Audience(Audience.Type.CLIENT)
@Recommended(
  reason =
      "Might be useful to override because of performance reasons in a "
          + "specific join types (e.g. sort join), which might reduce the space "
          + "complexity",
  state = StateComplexity.LINEAR,
  repartitions = 1
)
public class Join<LeftT, RightT, K, OutputT, W extends BoundedWindow>
    extends StateAwareWindowWiseOperator<
        Object, Either<LeftT, RightT>, K, Pair<K, OutputT>, W, Join<LeftT, RightT, K, OutputT, W>> {

  @SuppressWarnings("unchecked")
  private static final ListStorageDescriptor LEFT_STATE_DESCR =
      ListStorageDescriptor.of("left", (Class) Object.class);

  @SuppressWarnings("unchecked")
  private static final ListStorageDescriptor RIGHT_STATE_DESCR =
      ListStorageDescriptor.of("right", (Class) Object.class);

  @VisibleForTesting final UnaryFunction<LeftT, K> leftKeyExtractor;
  @VisibleForTesting final UnaryFunction<RightT, K> rightKeyExtractor;
  private final Dataset<LeftT> left;
  private final Dataset<RightT> right;
  private final Dataset<Pair<K, OutputT>> output;
  private final BinaryFunctor<LeftT, RightT, OutputT> functor;
  private final Type type;

  Join(
      String name,
      Flow flow,
      Dataset<LeftT> left,
      Dataset<RightT> right,
      UnaryFunction<LeftT, K> leftKeyExtractor,
      UnaryFunction<RightT, K> rightKeyExtractor,
      TypeDescriptor<K> keyType,
      BinaryFunctor<LeftT, RightT, OutputT> functor,
      TypeDescriptor<Pair<K, OutputT>> outputType,
      Type type,
      @Nullable WindowingDesc<Object, W> windowing,
      @Nullable Windowing euphoriaWindowing,
      Set<OutputHint> outputHints) {
    super(
        name,
        flow,
        outputType,
        windowing,
        euphoriaWindowing,
        (Either<LeftT, RightT> elem) -> {
          if (elem.isLeft()) {
            return leftKeyExtractor.apply(elem.left());
          }
          return rightKeyExtractor.apply(elem.right());
        },
        keyType);

    this.left = left;
    this.right = right;
    this.leftKeyExtractor = leftKeyExtractor;
    this.rightKeyExtractor = rightKeyExtractor;
    this.functor = functor;
    @SuppressWarnings("unchecked")
    Dataset<Pair<K, OutputT>> output = createOutput((Dataset) left, outputHints);
    this.output = output;
    this.type = type;
  }

  public static <LeftT, RightT> ByBuilder<LeftT, RightT> of(
      Dataset<LeftT> left, Dataset<RightT> right) {
    return new OfBuilder("Join").of(left, right);
  }

  /**
   * Name of join operator.
   *
   * @param name of operator
   * @return OfBuilder
   */
  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<Dataset<Object>> listInputs() {
    return Arrays.asList((Dataset) left, (Dataset) right);
  }

  @Override
  public Dataset<Pair<K, OutputT>> output() {
    return output;
  }

  public Type getType() {
    return type;
  }

  public UnaryFunction<LeftT, K> getLeftKeyExtractor() {
    return leftKeyExtractor;
  }

  public UnaryFunction<RightT, K> getRightKeyExtractor() {
    return rightKeyExtractor;
  }

  public BinaryFunctor<LeftT, RightT, OutputT> getJoiner() {
    return functor;
  }

  @Override
  @SuppressWarnings("unchecked")
  public DAG<Operator<?, ?>> getBasicOps() {
    final Flow flow = getFlow();

    TypeDescriptor<RightT> rightType = TypeUtils.getDatasetElementType(right);
    TypeDescriptor<LeftT> leftType = TypeUtils.getDatasetElementType(left);
    TypeDescriptor<Either<LeftT, RightT>> eitherLeftRightType =
        TypeUtils.eithers(leftType, rightType);

    final MapElements<LeftT, Either<LeftT, RightT>> leftMap =
        new MapElements<>(getName() + "::Map-left", flow, left, Either::left, eitherLeftRightType);

    final MapElements<RightT, Either<LeftT, RightT>> rightMap =
        new MapElements<>(
            getName() + "::Map-right", flow, right, Either::right, eitherLeftRightType);

    final Union<Either<LeftT, RightT>> union =
        new Union<>(
            getName() + "::Union",
            flow,
            Arrays.asList(leftMap.output(), rightMap.output()),
            eitherLeftRightType);

    final ReduceStateByKey<
            Either<LeftT, RightT>, K, Either<LeftT, RightT>, OutputT, StableJoinState, W>
        reduce =
            new ReduceStateByKey(
                getName() + "::ReduceStateByKey",
                flow,
                union.output(),
                keyExtractor,
                keyType,
                UnaryFunction.identity(),
                eitherLeftRightType,
                getWindowing(),
                euphoriaWindowing,
                (StateContext context, Collector ctx) -> {
                  StorageProvider storages = context.getStorageProvider();
                  return ctx == null
                      ? new StableJoinState(storages)
                      : new EarlyEmittingJoinState(storages, ctx);
                },
                new StateSupport.MergeFromStateMerger<>(),
                outputType,
                getHints());

    final DAG<Operator<?, ?>> dag = DAG.of(leftMap, rightMap);
    dag.add(union, leftMap, rightMap);
    dag.add(reduce, union);
    return dag;
  }

  /** Type of join. */
  public enum Type {
    INNER,
    LEFT,
    RIGHT,
    FULL
  }

  /** Parameters of this operator used in builders. */
  static class BuilderParams<LeftT, RightT, K, OutputT, W extends BoundedWindow>
      extends WindowingParams<W> {

    String name;
    Dataset<LeftT> left;
    Dataset<RightT> right;
    UnaryFunction<LeftT, K> leftKeyExtractor;
    UnaryFunction<RightT, K> rightKeyExtractor;
    TypeDescriptor<K> keyType;
    BinaryFunctor<LeftT, RightT, OutputT> joinFunc;
    TypeDescriptor<OutputT> outType;
    Type type;

    BuilderParams(String name, Dataset<LeftT> left, Dataset<RightT> right, Type type) {
      this.name = name;
      this.left = left;
      this.right = right;
      this.type = type;
    }
  }

  /** TODO: complete javadoc. */
  public static class OfBuilder {

    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    public <LeftT, RightT> ByBuilder<LeftT, RightT> of(Dataset<LeftT> left, Dataset<RightT> right) {
      if (right.getFlow() != left.getFlow()) {
        throw new IllegalArgumentException("Pass inputs from the same flow");
      }

      final BuilderParams<LeftT, RightT, ?, ?, ?> params =
          new BuilderParams<>(
              Objects.requireNonNull(name),
              Objects.requireNonNull(left),
              Objects.requireNonNull(right),
              Type.INNER);

      return new ByBuilder<>(params);
    }
  }

  /** TODO: complete javadoc. */
  public static class ByBuilder<LeftT, RightT> {

    private final BuilderParams<LeftT, RightT, ?, ?, ?> params;

    ByBuilder(BuilderParams<LeftT, RightT, ?, ?, ?> params) {
      this.params = params;
    }

    public <K> UsingBuilder<LeftT, RightT, K> by(
        UnaryFunction<LeftT, K> leftKeyExtractor,
        UnaryFunction<RightT, K> rightKeyExtractor,
        TypeDescriptor<K> keyTypeDescriptor) {

      @SuppressWarnings("unchecked")
      BuilderParams<LeftT, RightT, K, ?, ?> paramsCasted =
          (BuilderParams<LeftT, RightT, K, ?, ?>) this.params;

      paramsCasted.leftKeyExtractor = Objects.requireNonNull(leftKeyExtractor);
      paramsCasted.rightKeyExtractor = Objects.requireNonNull(rightKeyExtractor);
      paramsCasted.keyType = keyTypeDescriptor;
      return new UsingBuilder<>(paramsCasted);
    }

    public <K> UsingBuilder<LeftT, RightT, K> by(
        UnaryFunction<LeftT, K> leftKeyExtractor, UnaryFunction<RightT, K> rightKeyExtractor) {
      return by(leftKeyExtractor, rightKeyExtractor, null);
    }
  }

  /** TODO: complete javadoc. */
  public static class UsingBuilder<LeftT, RightT, K> {

    private final BuilderParams<LeftT, RightT, K, ?, ?> params;

    UsingBuilder(BuilderParams<LeftT, RightT, K, ?, ?> params) {
      this.params = params;
    }

    public <OutputT> Join.WindowingBuilder<LeftT, RightT, K, OutputT> using(
        BinaryFunctor<LeftT, RightT, OutputT> joinFunc,
        TypeDescriptor<OutputT> outputTypeDescriptor) {

      @SuppressWarnings("unchecked")
      BuilderParams<LeftT, RightT, K, OutputT, ?> paramsCasted =
          (BuilderParams<LeftT, RightT, K, OutputT, ?>) params;

      paramsCasted.joinFunc = Objects.requireNonNull(joinFunc);
      paramsCasted.outType = outputTypeDescriptor;

      return new Join.WindowingBuilder<>(paramsCasted);
    }

    public <OutputT> Join.WindowingBuilder<LeftT, RightT, K, OutputT> using(
        BinaryFunctor<LeftT, RightT, OutputT> joinFunc) {
      return using(joinFunc, null);
    }
  }

  /** TODO: complete javadoc. */
  public static class WindowingBuilder<LeftT, RightT, K, OutputT>
      implements Builders.Output<Pair<K, OutputT>>,
          Builders.OutputValues<K, OutputT>,
          OptionalMethodBuilder<
              WindowingBuilder<LeftT, RightT, K, OutputT>,
              OutputBuilder<LeftT, RightT, K, OutputT, ?>>,
          Builders.WindowBy<TriggerByBuilder<LeftT, RightT, K, OutputT, ?>> {

    private final BuilderParams<LeftT, RightT, K, OutputT, ?> params;

    WindowingBuilder(BuilderParams<LeftT, RightT, K, OutputT, ?> params) {
      this.params = params;
    }

    @Override
    public Dataset<Pair<K, OutputT>> output(OutputHint... outputHints) {
      return new OutputBuilder<>(params).output(outputHints);
    }

    @Override
    public <W extends BoundedWindow> TriggerByBuilder<LeftT, RightT, K, OutputT, W> windowBy(
        WindowFn<Object, W> windowing) {

      @SuppressWarnings("unchecked")
      BuilderParams<LeftT, RightT, K, OutputT, W> paramsCasted =
          (BuilderParams<LeftT, RightT, K, OutputT, W>) params;

      paramsCasted.windowFn = Objects.requireNonNull(windowing);

      return new TriggerByBuilder<>(paramsCasted);
    }

    @Override
    public <W extends Window<W>> OutputBuilder<LeftT, RightT, K, OutputT, ?> windowBy(
        Windowing<?, W> windowing) {
      params.euphoriaWindowing = Objects.requireNonNull(windowing);
      return new OutputBuilder<>(params);
    }

    @Override
    public OutputBuilder<LeftT, RightT, K, OutputT, ?> applyIf(
        boolean cond,
        UnaryFunction<
                WindowingBuilder<LeftT, RightT, K, OutputT>,
                OutputBuilder<LeftT, RightT, K, OutputT, ?>>
            applyWhenConditionHolds) {
      Objects.requireNonNull(applyWhenConditionHolds);

      if (cond) {
        return applyWhenConditionHolds.apply(this);
      }

      return new OutputBuilder<>(params);
    }
  }

  /** Trigger defining operator builder. */
  public static class TriggerByBuilder<LeftT, RightT, K, OutputT, W extends BoundedWindow>
      implements Builders.TriggeredBy<AccumulatorModeBuilder<LeftT, RightT, K, OutputT, W>> {

    private final BuilderParams<LeftT, RightT, K, OutputT, W> params;

    TriggerByBuilder(BuilderParams<LeftT, RightT, K, OutputT, W> params) {
      this.params = params;
    }

    @Override
    public AccumulatorModeBuilder<LeftT, RightT, K, OutputT, W> triggeredBy(Trigger trigger) {
      params.trigger = Objects.requireNonNull(trigger);
      return new AccumulatorModeBuilder<>(params);
    }
  }

  /** {@link WindowingStrategy.AccumulationMode} defining operator builder. */
  public static class AccumulatorModeBuilder<LeftT, RightT, K, OutputT, W extends BoundedWindow>
      implements Builders.AccumulatorMode<OutputBuilder<LeftT, RightT, K, OutputT, W>> {

    private final BuilderParams<LeftT, RightT, K, OutputT, W> params;

    AccumulatorModeBuilder(BuilderParams<LeftT, RightT, K, OutputT, W> params) {
      this.params = params;
    }

    @Override
    public OutputBuilder<LeftT, RightT, K, OutputT, W> accumulationMode(
        WindowingStrategy.AccumulationMode accumulationMode) {
      params.accumulationMode = Objects.requireNonNull(accumulationMode);
      return new OutputBuilder<>(params);
    }
  }

  /**
   * Last builder in a chain. It concludes this operators creation by calling {@link
   * #output(OutputHint...)}.
   */
  public static class OutputBuilder<LeftT, RightT, K, OutputT, W extends BoundedWindow>
      implements Builders.OutputValues<K, OutputT>, Builders.Output<Pair<K, OutputT>> {

    private final BuilderParams<LeftT, RightT, K, OutputT, W> params;

    OutputBuilder(BuilderParams<LeftT, RightT, K, OutputT, W> params) {
      this.params = params;
    }

    @Override
    public Dataset<Pair<K, OutputT>> output(OutputHint... outputHints) {
      final Flow flow = params.left.getFlow();
      final Join<LeftT, RightT, K, OutputT, W> join =
          new Join<>(
              params.name,
              flow,
              params.left,
              params.right,
              params.leftKeyExtractor,
              params.rightKeyExtractor,
              params.keyType,
              params.joinFunc,
              TypeUtils.pairs(params.keyType, params.outType),
              params.type,
              params.getWindowing(),
              params.euphoriaWindowing,
              Sets.newHashSet(outputHints));
      flow.add(join);
      return join.output();
    }
  }

  /** TODO: complete javadoc. */
  private abstract class AbstractJoinState implements State<Either<LeftT, RightT>, OutputT> {

    final ListStorage<LeftT> leftElements;
    final ListStorage<RightT> rightElements;

    @SuppressWarnings("unchecked")
    AbstractJoinState(StorageProvider storageProvider) {
      leftElements = storageProvider.getListStorage(LEFT_STATE_DESCR);
      rightElements = storageProvider.getListStorage(RIGHT_STATE_DESCR);
    }

    @Override
    public void close() {
      leftElements.clear();
      rightElements.clear();
    }

    /** This method can be triggered by all joins except INNER. */
    void flushUnjoinedElems(
        Collector<OutputT> context, Iterable<LeftT> lefts, Iterable<RightT> rights) {
      boolean leftEmpty = !lefts.iterator().hasNext();
      boolean rightEmpty = !rights.iterator().hasNext();
      // if just a one collection is empty
      if (leftEmpty != rightEmpty) {
        switch (getType()) {
          case LEFT:
            if (rightEmpty) {
              for (LeftT elem : lefts) {
                functor.apply(elem, null, context);
              }
            }
            break;
          case RIGHT:
            if (leftEmpty) {
              for (RightT elem : rights) {
                functor.apply(null, elem, context);
              }
            }
            break;
          case FULL:
            if (leftEmpty) {
              for (RightT elem : rights) {
                functor.apply(null, elem, context);
              }
            } else {
              for (LeftT elem : lefts) {
                functor.apply(elem, null, context);
              }
            }
            break;
          default:
            throw new IllegalArgumentException("Unsupported type: " + getType());
        }
      }
    }
  }

  /**
   * An implementation of the join state which will accumulate elements until it is flushed at which
   * point it then emits all elements.
   *
   * <p>(This implementation is known to work correctly with merging windowing, early triggering, as
   * well as with timed multi-window windowing (e.g. time sliding.))
   */
  private class StableJoinState extends AbstractJoinState
      implements StateSupport.MergeFrom<StableJoinState> {

    StableJoinState(StorageProvider storageProvider) {
      super(storageProvider);
    }

    @Override
    public void add(Either<LeftT, RightT> elem) {
      if (elem.isLeft()) {
        leftElements.add(elem.left());
      } else {
        rightElements.add(elem.right());
      }
    }

    @Override
    public void flush(Collector<OutputT> context) {
      Iterable<LeftT> lefts = leftElements.get();
      Iterable<RightT> rights = rightElements.get();
      for (LeftT l : lefts) {
        for (RightT r : rights) {
          functor.apply(l, r, context);
        }
      }
      if (type != Type.INNER) {
        flushUnjoinedElems(context, lefts, rights);
      }
    }

    @Override
    public void mergeFrom(StableJoinState other) {
      this.leftElements.addAll(other.leftElements.get());
      this.rightElements.addAll(other.rightElements.get());
    }
  }

  /**
   * An implementation of the join state which produces results, i.e. emits output, as soon as
   * possible. It has at least the following short comings and should be used with care (see
   * https://github.com/seznam/euphoria/issues/118 for more information):
   *
   * <ul>
   *   <li>This implementation will break the join operator if used with a merging windowing
   *       strategy, since items will be emitted under the hood of a non-final window.
   *   <li>This implementation cannot be used together with early triggering on any windowing
   *       strategy as it will emit each identified pair only once during the whole course of the
   *       state's life cycle.
   *   <li>This implementation will also break time-sliding windowing, as it will raise the
   *       watermark too quickly in downstream operators, thus, marking earlier - but actually still
   *       not too late time-sliding windows as late comers.
   * </ul>
   */
  @Experimental
  private class EarlyEmittingJoinState extends AbstractJoinState
      implements State<Either<LeftT, RightT>, OutputT>,
          StateSupport.MergeFrom<EarlyEmittingJoinState> {

    private final Collector<OutputT> context;

    @SuppressWarnings("unchecked")
    public EarlyEmittingJoinState(StorageProvider storageProvider, Collector<OutputT> context) {
      super(storageProvider);
      this.context = Objects.requireNonNull(context);
    }

    @Override
    public void add(Either<LeftT, RightT> elem) {
      if (elem.isLeft()) {
        leftElements.add(elem.left());
        emitJoinedElements(elem, rightElements);
      } else {
        rightElements.add(elem.right());
        emitJoinedElements(elem, leftElements);
      }
    }

    @SuppressWarnings("unchecked")
    private void emitJoinedElements(Either<LeftT, RightT> elem, ListStorage others) {
      assert context != null;
      if (elem.isLeft()) {
        for (Object right : others.get()) {
          functor.apply(elem.left(), (RightT) right, context);
        }
      } else {
        for (Object left : others.get()) {
          functor.apply((LeftT) left, elem.right(), context);
        }
      }
    }

    @Override
    public void flush(Collector<OutputT> context) {
      // ~ no-op; we do all the work already on the fly
      // and flush any "pending" state _only_ when closing
      // this state
    }

    @Override
    public void close() {
      if (type != Type.INNER) {
        flushUnjoinedElems(context, leftElements.get(), rightElements.get());
      }
      super.close();
    }

    @Override
    public void mergeFrom(EarlyEmittingJoinState other) {
      Iterable<LeftT> otherLefts = other.leftElements.get();
      Iterable<RightT> thisRights = this.rightElements.get();
      for (LeftT l : otherLefts) {
        for (RightT r : thisRights) {
          functor.apply(l, r, context);
        }
      }
      Iterable<RightT> otherRights = other.rightElements.get();
      Iterable<LeftT> thisLefts = this.leftElements.get();
      for (RightT r : otherRights) {
        for (LeftT l : thisLefts) {
          functor.apply(l, r, context);
        }
      }
      this.leftElements.addAll(otherLefts);
      this.rightElements.addAll(otherRights);
    }
  }
}
