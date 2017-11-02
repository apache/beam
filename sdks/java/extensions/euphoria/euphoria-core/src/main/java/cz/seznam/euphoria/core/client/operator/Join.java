/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.annotation.operator.Recommended;
import cz.seznam.euphoria.core.annotation.operator.StateComplexity;
import cz.seznam.euphoria.core.annotation.stability.Experimental;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.BinaryFunctor;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.util.Either;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.shaded.guava.com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Join two datasets by given key producing single new dataset.
 */
@Recommended(
    reason =
        "Might be useful to override because of performance reasons in a "
            + "specific join types (e.g. sort join), which might reduce the space "
            + "complexity",
    state = StateComplexity.LINEAR,
    repartitions = 1
)
public class Join<LEFT, RIGHT, KEY, OUT, W extends Window>
    extends StateAwareWindowWiseOperator<Object, Either<LEFT, RIGHT>,
    Either<LEFT, RIGHT>, KEY, Pair<KEY, OUT>, W,
    Join<LEFT, RIGHT, KEY, OUT, W>>
    implements HintAware<JoinHint> {

  public static class OfBuilder {
    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    public <LEFT, RIGHT> ByBuilder<LEFT, RIGHT>
    of(Dataset<LEFT> left, Dataset<RIGHT> right) {
      if (right.getFlow() != left.getFlow()) {
        throw new IllegalArgumentException("Pass inputs from the same flow");
      }
      return new ByBuilder<>(name, left, right);
    }
  }

  public static class ByBuilder<LEFT, RIGHT> {
    private final String name;
    private final Dataset<LEFT> left;
    private final Dataset<RIGHT> right;

    ByBuilder(String name, Dataset<LEFT> left, Dataset<RIGHT> right) {
      this.name = Objects.requireNonNull(name);
      this.left = Objects.requireNonNull(left);
      this.right = Objects.requireNonNull(right);
    }

    public <KEY> UsingBuilder<LEFT, RIGHT, KEY> by(
        UnaryFunction<LEFT, KEY> leftKeyExtractor,
        UnaryFunction<RIGHT, KEY> rightKeyExtractor) {

      return new UsingBuilder<>(name, left, right,
          leftKeyExtractor, rightKeyExtractor);
    }
  }

  public static class UsingBuilder<LEFT, RIGHT, KEY> {
    private final String name;
    private final Dataset<LEFT> left;
    private final Dataset<RIGHT> right;
    private final UnaryFunction<LEFT, KEY> leftKeyExtractor;
    private final UnaryFunction<RIGHT, KEY> rightKeyExtractor;

    UsingBuilder(String name,
                 Dataset<LEFT> left,
                 Dataset<RIGHT> right,
                 UnaryFunction<LEFT, KEY> leftKeyExtractor,
                 UnaryFunction<RIGHT, KEY> rightKeyExtractor) {

      this.name = name;
      this.left = left;
      this.right = right;
      this.leftKeyExtractor = leftKeyExtractor;
      this.rightKeyExtractor = rightKeyExtractor;
    }

    public <OUT> WindowingBuilder<LEFT, RIGHT, KEY, OUT> using(
        BinaryFunctor<LEFT, RIGHT, OUT> functor) {

      return new WindowingBuilder<>(name, left, right,
          leftKeyExtractor, rightKeyExtractor, functor,
          false, Collections.emptyList());
    }
  }

  public static class WindowingBuilder<LEFT, RIGHT, KEY, OUT>
      implements Builders.Output<Pair<KEY, OUT>>,
          OptionalMethodBuilder<WindowingBuilder<LEFT, RIGHT, KEY, OUT>> {

    private final String name;
    private final Dataset<LEFT> left;
    private final Dataset<RIGHT> right;
    private final UnaryFunction<LEFT, KEY> leftKeyExtractor;
    private final UnaryFunction<RIGHT, KEY> rightKeyExtractor;
    private final BinaryFunctor<LEFT, RIGHT, OUT> joinFunc;
    private final boolean outer;
    private final List<JoinHint> hints;

    WindowingBuilder(String name,
                     Dataset<LEFT> left,
                     Dataset<RIGHT> right,
                     UnaryFunction<LEFT, KEY> leftKeyExtractor,
                     UnaryFunction<RIGHT, KEY> rightKeyExtractor,
                     BinaryFunctor<LEFT, RIGHT, OUT> joinFunc,
                     boolean outer,
                     List<JoinHint> hints) {

      this.name = Objects.requireNonNull(name);
      this.left = Objects.requireNonNull(left);
      this.right = Objects.requireNonNull(right);
      this.leftKeyExtractor = Objects.requireNonNull(leftKeyExtractor);
      this.rightKeyExtractor = Objects.requireNonNull(rightKeyExtractor);
      this.joinFunc = Objects.requireNonNull(joinFunc);
      this.outer = outer;
      this.hints = Objects.requireNonNull(hints);
    }

    public WindowingBuilder<LEFT, RIGHT, KEY, OUT> outer() {
      return new WindowingBuilder<>(name, left, right, leftKeyExtractor,
          rightKeyExtractor, joinFunc, true, hints);
    }

    @Override
    public Dataset<Pair<KEY, OUT>> output() {
      return windowBy(null).output();
    }

    public <W extends Window>
    OutputBuilder<LEFT, RIGHT, KEY, OUT, W>
    windowBy(Windowing<Either<LEFT, RIGHT>, W> windowing) {
      return new OutputBuilder<>(name, left, right, leftKeyExtractor,
          rightKeyExtractor, joinFunc, outer, windowing, hints);
    }

    public WindowingBuilder<LEFT, RIGHT, KEY, OUT> withHints(JoinHint... hints) {
      return new WindowingBuilder<>(name, left, right, leftKeyExtractor,
          rightKeyExtractor, joinFunc, outer, Arrays.asList(hints));
    }
  }

  public static class OutputBuilder<
      LEFT, RIGHT, KEY, OUT, W extends Window>
      implements Builders.Output<Pair<KEY, OUT>> {

    private final String name;
    private final Dataset<LEFT> left;
    private final Dataset<RIGHT> right;
    private final UnaryFunction<LEFT, KEY> leftKeyExtractor;
    private final UnaryFunction<RIGHT, KEY> rightKeyExtractor;
    private final BinaryFunctor<LEFT, RIGHT, OUT> joinFunc;
    private final boolean outer;
    @Nullable
    private final Windowing<Either<LEFT, RIGHT>, W> windowing;
    private final List<JoinHint> hints;

    OutputBuilder(String name,
                  Dataset<LEFT> left,
                  Dataset<RIGHT> right,
                  UnaryFunction<LEFT, KEY> leftKeyExtractor,
                  UnaryFunction<RIGHT, KEY> rightKeyExtractor,
                  BinaryFunctor<LEFT, RIGHT, OUT> joinFunc,
                  boolean outer,
                  @Nullable Windowing<Either<LEFT, RIGHT>, W> windowing,
                  List<JoinHint> hints) {

      this.name = Objects.requireNonNull(name);
      this.left = Objects.requireNonNull(left);
      this.right = Objects.requireNonNull(right);
      this.leftKeyExtractor = Objects.requireNonNull(leftKeyExtractor);
      this.rightKeyExtractor = Objects.requireNonNull(rightKeyExtractor);
      this.joinFunc = Objects.requireNonNull(joinFunc);
      this.outer = outer;
      this.windowing = windowing;
      this.hints = hints;
    }

    public OutputBuilder<LEFT, RIGHT, KEY, OUT, W> withHints(JoinHint... hints) {
      return new OutputBuilder<>(name,
          left,
          right,
          leftKeyExtractor,
          rightKeyExtractor,
          joinFunc,
          outer,
          windowing,
          Arrays.asList(hints));
    }

    @Override
    public Dataset<Pair<KEY, OUT>> output() {
      Flow flow = left.getFlow();
      Join<LEFT, RIGHT, KEY, OUT, W> join =
          new Join<>(name, flow, left, right,
              windowing, leftKeyExtractor, rightKeyExtractor, joinFunc, outer,
              hints);
      flow.add(join);
      return join.output();
    }
  }

  public static <LEFT, RIGHT> ByBuilder<LEFT, RIGHT> of(
      Dataset<LEFT> left, Dataset<RIGHT> right) {

    return new OfBuilder("Join").of(left, right);
  }

  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  private final Dataset<LEFT> left;
  private final Dataset<RIGHT> right;
  private final Dataset<Pair<KEY, OUT>> output;
  private final BinaryFunctor<LEFT, RIGHT, OUT> functor;
  @VisibleForTesting
  final UnaryFunction<LEFT, KEY> leftKeyExtractor;
  @VisibleForTesting
  final UnaryFunction<RIGHT, KEY> rightKeyExtractor;
  @VisibleForTesting
  final boolean outer;
  private final List<JoinHint> hints;

  Join(String name,
       Flow flow,
       Dataset<LEFT> left, Dataset<RIGHT> right,
       @Nullable Windowing<Either<LEFT, RIGHT>, W> windowing,
       UnaryFunction<LEFT, KEY> leftKeyExtractor,
       UnaryFunction<RIGHT, KEY> rightKeyExtractor,
       BinaryFunctor<LEFT, RIGHT, OUT> functor,
       boolean outer,
       List<JoinHint> hints) {

    super(name, flow, windowing, (Either<LEFT, RIGHT> elem) -> {
      if (elem.isLeft()) {
        return leftKeyExtractor.apply(elem.left());
      }
      return rightKeyExtractor.apply(elem.right());
    });
    this.left = left;
    this.right = right;
    this.leftKeyExtractor = leftKeyExtractor;
    this.rightKeyExtractor = rightKeyExtractor;
    this.functor = functor;
    @SuppressWarnings("unchecked")
    Dataset<Pair<KEY, OUT>> output = createOutput((Dataset) left);
    this.output = output;
    this.outer = outer;
    this.hints = hints;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<Dataset<Object>> listInputs() {
    return Arrays.asList((Dataset) left, (Dataset) right);
  }

  @Override
  public Dataset<Pair<KEY, OUT>> output() {
    return output;
  }

  @SuppressWarnings("unchecked")
  private static final ListStorageDescriptor LEFT_STATE_DESCR =
      ListStorageDescriptor.of("left", (Class) Object.class);
  @SuppressWarnings("unchecked")
  private static final ListStorageDescriptor RIGHT_STATE_DESCR =
      ListStorageDescriptor.of("right", (Class) Object.class);


  private abstract class AbstractJoinState implements State<Either<LEFT, RIGHT>, OUT> {

    final ListStorage<LEFT> leftElements;
    final ListStorage<RIGHT> rightElements;

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

    void flushUnjoinedElems(
        Collector<OUT> context, Iterable<LEFT> lefts, Iterable<RIGHT> rights) {
      boolean leftEmpty = !lefts.iterator().hasNext();
      boolean rightEmpty = !rights.iterator().hasNext();
      // if just a one collection is empty
      if (leftEmpty != rightEmpty) {
        if (leftEmpty) {
          for (RIGHT elem : rights) {
            functor.apply(null, elem, context);
          }
        } else {
          for (LEFT elem : lefts) {
            functor.apply(elem, null, context);
          }
        }
      }
    }
  }

  /**
   * An implementation of the join state which will accumulate elements
   * until it is flushed at which point it then emits all elements.<p>
   *
   * (This implementation is known to work correctly with merging
   * windowing, early triggering, as well as with timed multi-window
   * windowing (e.g. time sliding.))
   */
  private class StableJoinState extends AbstractJoinState
      implements StateSupport.MergeFrom<StableJoinState> {

    StableJoinState(StorageProvider storageProvider) {
      super(storageProvider);
    }

    @Override
    public void add(Either<LEFT, RIGHT> elem) {
      if (elem.isLeft()) {
        leftElements.add(elem.left());
      } else {
        rightElements.add(elem.right());
      }
    }

    @Override
    public void flush(Collector<OUT> context) {
      Iterable<LEFT> lefts = leftElements.get();
      Iterable<RIGHT> rights = rightElements.get();
      for (LEFT l : lefts) {
        for (RIGHT r : rights) {
          functor.apply(l, r, context);
        }
      }
      if (outer) {
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
   * An implementation of the join state which produces results, i.e. emits
   * output, as soon as possible. It has at least the following short comings
   * and should be used with care (see https://github.com/seznam/euphoria/issues/118
   * for more information):
   *
   * <ul>
   *   <li>This implementation will break the join operator if used with a
   *        merging windowing strategy, since items will be emitted under the
   *        hood of a non-final window.</li>
   *   <li>This implementation cannot be used together with early triggering
   *        on any windowing strategy as it will emit each identified pair
   *        only once during the whole course of the state's life cycle.</li>
   *   <li>This implementation will also break time-sliding windowing, as
   *        it will raise the watermark too quickly in downstream operators,
   *        thus, marking earlier - but actually still not too late time-sliding
   *        windows as late comers.</li>
   * </ul>
   *
   *
   */
  @Experimental
  private class EarlyEmittingJoinState
      extends AbstractJoinState
      implements State<Either<LEFT, RIGHT>, OUT>,
      StateSupport.MergeFrom<EarlyEmittingJoinState> {
    private final Collector<OUT> context;

    @SuppressWarnings("unchecked")
    public EarlyEmittingJoinState(StorageProvider storageProvider, Collector<OUT> context) {
      super(storageProvider);
      this.context = Objects.requireNonNull(context);
    }

    @Override
    public void add(Either<LEFT, RIGHT> elem) {
      if (elem.isLeft()) {
        leftElements.add(elem.left());
        emitJoinedElements(elem, rightElements);
      } else {
        rightElements.add(elem.right());
        emitJoinedElements(elem, leftElements);
      }
    }

    @SuppressWarnings("unchecked")
    private void emitJoinedElements(Either<LEFT, RIGHT> elem, ListStorage others) {
      assert context != null;
      if (elem.isLeft()) {
        for (Object right : others.get()) {
          functor.apply(elem.left(), (RIGHT) right, context);
        }
      } else {
        for (Object left : others.get()) {
          functor.apply((LEFT) left, elem.right(), context);
        }
      }
    }

    @Override
    public void flush(Collector<OUT> context) {
      // ~ no-op; we do all the work already on the fly
      // and flush any "pending" state _only_ when closing
      // this state
    }

    @Override
    public void close() {
      if (outer) {
        flushUnjoinedElems(context, leftElements.get(), rightElements.get());
      }
      super.close();
    }

    @Override
    public void mergeFrom(EarlyEmittingJoinState other) {
      Iterable<LEFT> otherLefts = other.leftElements.get();
      Iterable<RIGHT> thisRights = this.rightElements.get();
      for (LEFT l : otherLefts) {
        for (RIGHT r : thisRights) {
          functor.apply(l, r, context);
        }
      }
      Iterable<RIGHT> otherRights = other.rightElements.get();
      Iterable<LEFT> thisLefts = this.leftElements.get();
      for (RIGHT r : otherRights) {
        for (LEFT l : thisLefts) {
          functor.apply(l, r, context);
        }
      }
      this.leftElements.addAll(otherLefts);
      this.rightElements.addAll(otherRights);
    }
  }

  public boolean isOuter() {
    return outer;
  }

  public UnaryFunction<LEFT, KEY> getLeftKeyExtractor() {
    return leftKeyExtractor;
  }

  public UnaryFunction<RIGHT, KEY> getRightKeyExtractor() {
    return rightKeyExtractor;
  }

  public BinaryFunctor<LEFT, RIGHT, OUT> getJoiner() {
    return functor;
  }

  @Override
  public List<JoinHint> getHints() {
    return hints;
  }

  @Override
  @SuppressWarnings("unchecked")
  public DAG<Operator<?, ?>> getBasicOps() {
    Flow flow = getFlow();

    String name = getName() + "::Map-left";
    MapElements<LEFT, Either<LEFT, RIGHT>> leftMap = new MapElements<>(
        name, flow, left, Either::left);

    name = getName() + "::Map-right";
    MapElements<RIGHT, Either<LEFT, RIGHT>> rightMap = new MapElements<>(
        name, flow, right, Either::right);

    name = getName() + "::Union";
    Union<Either<LEFT, RIGHT>> union =
        new Union<>(name, flow, leftMap.output(), rightMap.output());

    ReduceStateByKey<Either<LEFT, RIGHT>, KEY, Either<LEFT, RIGHT>, OUT, StableJoinState, W>
        reduce = new ReduceStateByKey(
        getName() + "::ReduceStateByKey",
        flow,
        union.output(),
        keyExtractor,
        e -> e,
        getWindowing(),
        (StorageProvider storages, Collector ctx) ->
            ctx == null
                ? new StableJoinState(storages)
                : new EarlyEmittingJoinState(storages, ctx),
        new StateSupport.MergeFromStateMerger<>());

    DAG<Operator<?, ?>> dag = DAG.of(leftMap, rightMap);
    dag.add(union, leftMap, rightMap);
    dag.add(reduce, union);
    return dag;
  }

}
