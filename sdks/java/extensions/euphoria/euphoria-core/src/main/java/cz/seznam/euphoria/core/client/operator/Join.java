
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.BatchWindowing;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.dataset.Window;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.BinaryFunctor;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.util.Either;
import cz.seznam.euphoria.core.client.util.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;

/**
 * Join two datasets by given key producing single new dataset.
 */
public class Join<LEFT, RIGHT, KEY, OUT, WLABEL, W extends Window<?, WLABEL>,
                  PAIROUT extends Pair<KEY, OUT>>
    extends StateAwareWindowWiseOperator<Object, Either<LEFT, RIGHT>,
    Either<LEFT, RIGHT>, KEY, PAIROUT, WLABEL, W,
    Join<LEFT, RIGHT, KEY, OUT, WLABEL, W, PAIROUT>>
    implements OutputBuilder<PAIROUT>
{

  public static class OfBuilder {
    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    public <LEFT, RIGHT> ByBuilder<LEFT, RIGHT> of(
            Dataset<LEFT> left, Dataset<RIGHT> right)
    {
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
        UnaryFunction<RIGHT, KEY> rightKeyExtractor)
    {
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
                 UnaryFunction<RIGHT, KEY> rightKeyExtractor)
    {
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
      implements cz.seznam.euphoria.core.client.operator.OutputBuilder<Pair<KEY, OUT>>
  {
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
                     BinaryFunctor<LEFT, RIGHT, OUT> joinFunc)
    {
      // define default partitioning
      super(new DefaultPartitioning<>(Math.max(
              left.getPartitioning().getNumPartitions(),
              right.getPartitioning().getNumPartitions())));

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
      return new OutputBuilder<>(this, BatchWindowing.get()).output();
    }

    public <WLABEL, W extends Window<?, WLABEL>>
    OutputBuilder<LEFT, RIGHT, KEY, OUT, WLABEL, W>
    windowBy(Windowing<Either<LEFT, RIGHT>, ?, WLABEL, W> windowing)
    {
      return new OutputBuilder<>(this, windowing);
    }
  }

  public static class OutputBuilder<
      LEFT, RIGHT, KEY, OUT, WLABEL, W extends Window<?, WLABEL>>
      implements cz.seznam.euphoria.core.client.operator.OutputBuilder<Pair<KEY, OUT>>
  {
    private final WindowingBuilder<LEFT, RIGHT, KEY, OUT> prev;
    private final Windowing<Either<LEFT, RIGHT>, ?, WLABEL, W> windowing;

    OutputBuilder(WindowingBuilder<LEFT, RIGHT, KEY, OUT> prev,
                  Windowing<Either<LEFT, RIGHT>, ?, WLABEL, W> windowing)
    {
      this.prev = prev;
      this.windowing = windowing;
    }

    @Override
    public Dataset<Pair<KEY, OUT>> output() {
      return (Dataset) outputWindowed();
    }

    public Dataset<WindowedPair<WLABEL, KEY, OUT>> outputWindowed() {
      Flow flow = prev.left.getFlow();
      Join<LEFT, RIGHT, KEY, OUT, WLABEL, W, WindowedPair<WLABEL, KEY, OUT>> join =
          new Join<>(prev.name, flow, prev.left, prev.right,
          windowing, prev.getPartitioning(),
          prev.leftKeyExtractor, prev.rightKeyExtractor, prev.joinFunc, prev.outer);
      flow.add(join);

      return join.output();
    }
  }

  public static <LEFT, RIGHT> ByBuilder<LEFT, RIGHT> of(
      Dataset<LEFT> left, Dataset<RIGHT> right)
  {
    return new OfBuilder("Join").of(left, right);
  }

  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  private final Dataset<LEFT> left;
  private final Dataset<RIGHT> right;
  private final Dataset<PAIROUT> output;
  private final BinaryFunctor<LEFT, RIGHT, OUT> functor;
  final UnaryFunction<LEFT, KEY> leftKeyExtractor;
  final UnaryFunction<RIGHT, KEY> rightKeyExtractor;
  boolean outer = false;

  Join(String name,
      Flow flow,
      Dataset<LEFT> left, Dataset<RIGHT> right,
      Windowing<Either<LEFT, RIGHT>, ?, WLABEL, W> windowing,
      Partitioning<KEY> partitioning,
      UnaryFunction<LEFT, KEY> leftKeyExtractor,
      UnaryFunction<RIGHT, KEY> rightKeyExtractor,
      BinaryFunctor<LEFT, RIGHT, OUT> functor,
      boolean outer)
  {
    super(name, flow, windowing, (Either<LEFT, RIGHT> elem) -> {
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
    this.output = createOutput((Dataset) left);
    this.outer = outer;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<Dataset<Object>> listInputs() {
    return Arrays.asList((Dataset) left, (Dataset) right);
  }

  @Override
  public Dataset<PAIROUT> output() {
    return output;
  }

  // keeper of state for window
  private class JoinState extends State<Either<LEFT, RIGHT>, OUT> {

    // store the elements in memory for this implementation
    final Collection<LEFT> leftElements = new ArrayList<>();
    final Collection<RIGHT> rightElements = new ArrayList<>();
    final Collector<OUT> functorCollector = getCollector()::collect;

    public JoinState(Collector<OUT> collector) {
      super(collector);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void add(Either<LEFT, RIGHT> element) {
      if (element.isLeft()) {
        leftElements.add(element.left());
        emitJoinedElements(element, (Collection) rightElements);
      } else {
        rightElements.add(element.right());
        emitJoinedElements(element, (Collection) leftElements);
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
    }

    private void flushUnjoinedElems() {
      if (leftElements.isEmpty() ^ rightElements.isEmpty()) {
        // if just a one collection is empty
        if (leftElements.isEmpty()) {
          for (RIGHT elem : rightElements) {
            functor.apply(null, elem, functorCollector);
          }
        } else {
          for (LEFT elem : leftElements) {
            functor.apply(elem, null, functorCollector);
          }
        }
      }
    }

    @SuppressWarnings("unchecked")
    private void emitJoinedElements(
        Either<LEFT, RIGHT> element, Collection<Object> otherElements) {
      if (element.isLeft()) {
        for (Object right : otherElements) {
          functor.apply(element.left(), (RIGHT) right, functorCollector);
        }
      } else {
        for (Object left : otherElements) {
          functor.apply((LEFT) left, element.right(), functorCollector);
        }
      }
    }

    JoinState merge(Iterator<JoinState> i) {
      while (i.hasNext()) {
        JoinState state = i.next();
        for (LEFT l : state.leftElements) {
          for (RIGHT r : this.rightElements) {
            functor.apply(l, r, functorCollector);
          }
        }
        for (RIGHT r : state.rightElements) {
          for (LEFT l : this.leftElements) {
            functor.apply(l, r, functorCollector);
          }
        }
        this.leftElements.addAll(state.leftElements);
        this.rightElements.addAll(state.rightElements);
      }
      return this;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public DAG<Operator<?, ?>> getBasicOps() {
    Flow flow = getFlow();

    String name = getName() + "::" + "Map-left";
    MapElements<LEFT, Either<LEFT, RIGHT>> leftMap = new MapElements<>(name, flow, left, Either::left);

    name = getName() + "::" + "Map-right";
    MapElements<RIGHT, Either<LEFT, RIGHT>> rightMap = new MapElements<>(name, flow, right, Either::right);

    name = getName() + "::" + "Union";
    Union<Either<LEFT, RIGHT>> union =
        new Union<>(name, flow, leftMap.output(), rightMap.output());

    ReduceStateByKey<Either<LEFT, RIGHT>, Either<LEFT, RIGHT>, Either<LEFT, RIGHT>,
        KEY, Either<LEFT, RIGHT>, KEY,
        OUT, JoinState, WLABEL, W, ?> reduce;

    name = getName() + "::" + "ReduceStateByKey";
    reduce = new ReduceStateByKey<>(
              name,
              flow,
              union.output(),
              keyExtractor::apply,
              e -> e,
              getWindowing(),
              JoinState::new,
              (Iterable<JoinState> states) -> {
                Iterator<JoinState> iter = states.iterator();
                final JoinState first;
                if (iter.hasNext()) {
                  first = iter.next();
                } else {
                  // this is strange
                  throw new IllegalStateException("Reducing empty states?");
                }
                return first.merge(iter);
              },
              partitioning
        );

    DAG<Operator<?, ?>> dag = DAG.of(leftMap, rightMap);
    dag.add(union, leftMap, rightMap);
    dag.add(reduce, union);
    return dag;
  }

}
