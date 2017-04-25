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
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.BinaryFunctor;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.util.Either;
import cz.seznam.euphoria.core.client.util.Pair;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
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
    implements OutputBuilder<Pair<KEY, OUT>> {

  public static class OfBuilder {
    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    public <LEFT, RIGHT> ByBuilder<LEFT, RIGHT> of(
            Dataset<LEFT> left, Dataset<RIGHT> right) {
      
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
            BinaryFunctor<LEFT, RIGHT, OUT> functor)
    {
      return new WindowingBuilder<>(name, left, right,
              leftKeyExtractor, rightKeyExtractor, functor);
    }
  }

  public static class WindowingBuilder<LEFT, RIGHT, KEY, OUT>
      extends PartitioningBuilder<KEY, WindowingBuilder<LEFT, RIGHT, KEY, OUT>>
      implements cz.seznam.euphoria.core.client.operator.OutputBuilder<Pair<KEY, OUT>> {

    private final String name;
    private final Dataset<LEFT> left;
    private final Dataset<RIGHT> right;
    private final UnaryFunction<LEFT, KEY> leftKeyExtractor;
    private final UnaryFunction<RIGHT, KEY> rightKeyExtractor;
    private final BinaryFunctor<LEFT, RIGHT, OUT> joinFunc;
    private boolean outer;

    WindowingBuilder(String name,
                     Dataset<LEFT> left,
                     Dataset<RIGHT> right,
                     UnaryFunction<LEFT, KEY> leftKeyExtractor,
                     UnaryFunction<RIGHT, KEY> rightKeyExtractor,
                     BinaryFunctor<LEFT, RIGHT, OUT> joinFunc) {
      
      // define default partitioning
      super(new DefaultPartitioning<>(
          Math.max(left.getNumPartitions(), right.getNumPartitions())));

      this.name = Objects.requireNonNull(name);
      this.left = Objects.requireNonNull(left);
      this.right = Objects.requireNonNull(right);
      this.leftKeyExtractor = Objects.requireNonNull(leftKeyExtractor);
      this.rightKeyExtractor = Objects.requireNonNull(rightKeyExtractor);
      this.joinFunc = Objects.requireNonNull(joinFunc);
    }

    public WindowingBuilder<LEFT, RIGHT, KEY, OUT> outer() {
      this.outer = true;
      return this;
    }

    @Override
    public Dataset<Pair<KEY, OUT>> output() {
      return windowBy(null, null, null).output();
    }

    public <W extends Window>
    OutputBuilder<LEFT, RIGHT, KEY, OUT, W>
    windowBy(Windowing<Either<LEFT, RIGHT>, W> windowing)
    {
      return windowBy(windowing, null, null);
    }

    public <W extends Window>
    OutputBuilder<LEFT, RIGHT, KEY, OUT, W>
    windowBy(Windowing<Either<LEFT, RIGHT>, W> windowing,
             ExtractEventTime<LEFT> leftEventTimeFn,
             ExtractEventTime<RIGHT> rightEventTimeFn) {

      ExtractEventTime<Either<LEFT, RIGHT>> eventTimeAssigner = null;

      if (leftEventTimeFn != null || rightEventTimeFn != null) {
        Objects.requireNonNull(leftEventTimeFn);
        Objects.requireNonNull(rightEventTimeFn);
        eventTimeAssigner = either -> either.isLeft()
                ? leftEventTimeFn.extractTimestamp(either.left())
                : rightEventTimeFn.extractTimestamp(either.right());
      }

      return new OutputBuilder<>(name, left, right, leftKeyExtractor,
              rightKeyExtractor, joinFunc, outer, this, windowing, eventTimeAssigner);
    }
  }

  public static class OutputBuilder<
      LEFT, RIGHT, KEY, OUT, W extends Window>
      extends PartitioningBuilder<KEY, OutputBuilder<LEFT, RIGHT, KEY, OUT, W>>
      implements cz.seznam.euphoria.core.client.operator.OutputBuilder<Pair<KEY, OUT>> {

    private final String name;
    private final Dataset<LEFT> left;
    private final Dataset<RIGHT> right;
    private final UnaryFunction<LEFT, KEY> leftKeyExtractor;
    private final UnaryFunction<RIGHT, KEY> rightKeyExtractor;
    private final BinaryFunctor<LEFT, RIGHT, OUT> joinFunc;
    private final boolean outer;
    @Nullable
    private final Windowing<Either<LEFT, RIGHT>, W> windowing;
    @Nullable
    private final ExtractEventTime<Either<LEFT, RIGHT>> eventTimeAssigner;

    OutputBuilder(String name,
                  Dataset<LEFT> left,
                  Dataset<RIGHT> right,
                  UnaryFunction<LEFT, KEY> leftKeyExtractor,
                  UnaryFunction<RIGHT, KEY> rightKeyExtractor,
                  BinaryFunctor<LEFT, RIGHT, OUT> joinFunc,
                  boolean outer,
                  PartitioningBuilder<KEY, ?> partitioning,
                  @Nullable Windowing<Either<LEFT, RIGHT>, W> windowing,
                  @Nullable ExtractEventTime<Either<LEFT, RIGHT>> eventTimeAssigner) {

      super(partitioning);

      this.name = Objects.requireNonNull(name);
      this.left = Objects.requireNonNull(left);
      this.right = Objects.requireNonNull(right);
      this.leftKeyExtractor = Objects.requireNonNull(leftKeyExtractor);
      this.rightKeyExtractor = Objects.requireNonNull(rightKeyExtractor);
      this.joinFunc = Objects.requireNonNull(joinFunc);
      this.outer = outer;
      this.windowing = windowing;
      this.eventTimeAssigner = eventTimeAssigner;
    }

    @Override
    public Dataset<Pair<KEY, OUT>> output() {
      Flow flow = left.getFlow();
      Join<LEFT, RIGHT, KEY, OUT, W> join =
          new Join<>(name, flow, left, right,
              windowing, eventTimeAssigner, getPartitioning(),
              leftKeyExtractor, rightKeyExtractor, joinFunc, outer);
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
  final UnaryFunction<LEFT, KEY> leftKeyExtractor;
  final UnaryFunction<RIGHT, KEY> rightKeyExtractor;
  boolean outer = false;

  Join(String name,
       Flow flow,
       Dataset<LEFT> left, Dataset<RIGHT> right,
       @Nullable Windowing<Either<LEFT, RIGHT>, W> windowing,
       @Nullable ExtractEventTime<Either<LEFT, RIGHT>> eventTimeAssigner,
       Partitioning<KEY> partitioning,
       UnaryFunction<LEFT, KEY> leftKeyExtractor,
       UnaryFunction<RIGHT, KEY> rightKeyExtractor,
       BinaryFunctor<LEFT, RIGHT, OUT> functor,
       boolean outer) {

    super(name, flow, windowing, eventTimeAssigner, (Either<LEFT, RIGHT> elem) -> {
      if (elem.isLeft()) {
        return leftKeyExtractor.apply(elem.left());
      }
      return rightKeyExtractor.apply(elem.right());
    }, partitioning);
    this.left = left;
    this.right = right;
    this.leftKeyExtractor = leftKeyExtractor;
    this.rightKeyExtractor = rightKeyExtractor;
    this.functor = functor;
    @SuppressWarnings("unchecked")
    Dataset<Pair<KEY, OUT>> output = createOutput((Dataset) left);
    this.output = output;
    this.outer = outer;
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
  static final ListStorageDescriptor LEFT_STATE_DESCR =
          ListStorageDescriptor.of("left", (Class) Object.class);
  @SuppressWarnings("unchecked")
  static final ListStorageDescriptor RIGHT_STATE_DESCR =
          ListStorageDescriptor.of("right", (Class) Object.class);

  private class JoinState
          extends State<Either<LEFT, RIGHT>, OUT>
          implements StateSupport.MergeFrom<JoinState> {

    // store the elements in memory for this implementation
    final ListStorage<LEFT> leftElements;
    final ListStorage<RIGHT> rightElements;

    @SuppressWarnings("unchecked")
    public JoinState(Context<OUT> context, StorageProvider storageProvider) {
      super(context);
      leftElements = storageProvider.getListStorage(LEFT_STATE_DESCR);
      rightElements = storageProvider.getListStorage(RIGHT_STATE_DESCR);
    }

    @Override
    public void add(Either<LEFT, RIGHT> element) {
      if (element.isLeft()) {
        leftElements.add(element.left());
        emitJoinedElements(element, rightElements);
      } else {
        rightElements.add(element.right());
        emitJoinedElements(element, leftElements);
      }
    }

    @Override
    public void flush() {
      // ~ no-op; we do all the work already on the fly
      // and flush any "pending" state _only_ when closing
      // this state
    }

    @Override
    public void close() {
      if (outer) {
        flushUnjoinedElems();
      }
      leftElements.clear();
      rightElements.clear();
    }

    private void flushUnjoinedElems() {
      Iterable<LEFT> lefts = leftElements.get();
      Iterable<RIGHT> rights = rightElements.get();

      boolean leftEmpty = !lefts.iterator().hasNext();
      boolean rightEmpty = !rights.iterator().hasNext();
      if (leftEmpty ^ rightEmpty) {
        // if just a one collection is empty
        if (leftEmpty) {
          for (RIGHT elem : rights) {
            functor.apply(null, elem, getContext());
          }
        } else {
          for (LEFT elem : lefts) {
            functor.apply(elem, null, getContext());
          }
        }
      }
    }

    @SuppressWarnings("unchecked")
    private void emitJoinedElements(
        Either<LEFT, RIGHT> element, ListStorage otherElements) {
      if (element.isLeft()) {
        for (Object right : otherElements.get()) {
          functor.apply(element.left(), (RIGHT) right, getContext());
        }
      } else {
        for (Object left : otherElements.get()) {
          functor.apply((LEFT) left, element.right(), getContext());
        }
      }
    }

    @Override
    public void mergeFrom(JoinState other) {
      // TODO retrieving the actual list stored in the state is a costly operation
      // ... optimize for it (avoid needlessly calling storage.get(..) multiple times)
      // ... also avoid calling addAll or alternatively provide a more efficient impl
      for (LEFT l : other.leftElements.get()) {
        for (RIGHT r : this.rightElements.get()) {
          functor.apply(l, r, getContext());
        }
      }
      for (RIGHT r : other.rightElements.get()) {
        for (LEFT l : this.leftElements.get()) {
          functor.apply(l, r, getContext());
        }
      }
      this.leftElements.addAll(other.leftElements.get());
      this.rightElements.addAll(other.rightElements.get());
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

    ReduceStateByKey<Either<LEFT, RIGHT>, KEY, Either<LEFT, RIGHT>, OUT, JoinState, W>
        reduce = new ReduceStateByKey(
              getName() + "::ReduceStateByKey",
              flow,
              union.output(),
              keyExtractor,
              e -> e,
              getWindowing(),
              getEventTimeAssigner(),
              JoinState::new,
              new StateSupport.MergeFromStateMerger<>(),
              partitioning
        );

    DAG<Operator<?, ?>> dag = DAG.of(leftMap, rightMap);
    dag.add(union, leftMap, rightMap);
    dag.add(reduce, union);
    return dag;
  }

}
