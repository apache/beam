
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.BatchWindowing;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.HashPartitioning;
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

/**
 * Join two datasets by given key producing single new dataset.
 */
public class Join<LEFT, RIGHT, KEY, OUT, W extends Window<?, ?>>
    extends StateAwareWindowWiseOperator<Object, Either<LEFT, RIGHT>,
    Either<LEFT, RIGHT>, KEY, Pair<KEY, OUT>, W,
    Join<LEFT, RIGHT, KEY, OUT, W>> {

  public static class Builder1<LEFT, RIGHT> {
    final Dataset<LEFT> left;
    final Dataset<RIGHT> right;
    Builder1(Dataset<LEFT> left, Dataset<RIGHT> right) {
      this.left = left;
      this.right = right;
    }
    public <KEY> Builder2<LEFT, RIGHT, KEY> by(
        UnaryFunction<LEFT, KEY> leftKeyExtractor,
        UnaryFunction<RIGHT, KEY> rightKeyExtractor) {
      return new Builder2<>(left, right, leftKeyExtractor, rightKeyExtractor);
    }
  }
  public static class Builder2<LEFT, RIGHT, KEY> {
    final Dataset<LEFT> left;
    final Dataset<RIGHT> right;
    final UnaryFunction<LEFT, KEY> leftKeyExtractor;
    final UnaryFunction<RIGHT, KEY> rightKeyExtractor;
    Builder2(Dataset<LEFT> left, Dataset<RIGHT> right,
        UnaryFunction<LEFT, KEY> leftKeyExtractor,
        UnaryFunction<RIGHT, KEY> rightKeyExtractor) {
      this.left = left;
      this.right = right;
      this.leftKeyExtractor = leftKeyExtractor;
      this.rightKeyExtractor = rightKeyExtractor;
    }
    public <OUT> Builder3<LEFT, RIGHT, KEY, OUT> using(
        BinaryFunctor<LEFT, RIGHT, OUT> functor) {
      return new Builder3<>(
          left, right, leftKeyExtractor, rightKeyExtractor, functor);
    }
  }
  public static class Builder3<LEFT, RIGHT, KEY, OUT> {
    final Dataset<LEFT> left;
    final Dataset<RIGHT> right;
    final UnaryFunction<LEFT, KEY> leftKeyExtractor;
    final UnaryFunction<RIGHT, KEY> rightKeyExtractor;
    final BinaryFunctor<LEFT, RIGHT, OUT> joinFunc;
    Builder3(Dataset<LEFT> left, Dataset<RIGHT> right,
        UnaryFunction<LEFT, KEY> leftKeyExtractor,
        UnaryFunction<RIGHT, KEY> rightKeyExtractor,
        BinaryFunctor<LEFT, RIGHT, OUT> joinFunc) {
      this.left = left;
      this.right = right;
      this.leftKeyExtractor = leftKeyExtractor;
      this.rightKeyExtractor = rightKeyExtractor;
      this.joinFunc = joinFunc;
    }
    public <W extends Window<?, ?>> Join<LEFT, RIGHT, KEY, OUT, W>
    windowBy(Windowing<Either<LEFT, RIGHT>, ?, ?, W> windowing) {
      Flow flow = left.getFlow();
      Join<LEFT, RIGHT, KEY, OUT, W> join = new Join<>(
          flow, left, right, windowing, leftKeyExtractor, rightKeyExtractor, joinFunc);
      return flow.add(join);
    }
    public Dataset<Pair<KEY, OUT>> output() {
      return windowBy(BatchWindowing.get()).output();
    }

  }

  public static <LEFT, RIGHT> Builder1<LEFT, RIGHT> of(
      Dataset<LEFT> left, Dataset<RIGHT> right) {
    if (right.getFlow() != left.getFlow()) {
      throw new IllegalArgumentException("Pass inputs from the same flow");
    }
    return new Builder1<>(left, right);
  }

  private final Dataset<LEFT> left;
  private final Dataset<RIGHT> right;
  private final Dataset<Pair<KEY, OUT>> output;
  private final BinaryFunctor<LEFT, RIGHT, OUT> functor;
  final UnaryFunction<LEFT, KEY> leftKeyExtractor;
  final UnaryFunction<RIGHT, KEY> rightKeyExtractor;

  private boolean outer = false;

  @SuppressWarnings("unchecked")
  Join(Flow flow,
      Dataset<LEFT> left, Dataset<RIGHT> right,
      Windowing<Either<LEFT, RIGHT>, ?, ?, W> windowing,
      UnaryFunction<LEFT, KEY> leftKeyExtractor,
      UnaryFunction<RIGHT, KEY> rightKeyExtractor,
      BinaryFunctor<LEFT, RIGHT, OUT> functor) {

    super("Join", flow, windowing, (Either<LEFT, RIGHT> elem) -> {
      if (elem.isLeft()) {
        return leftKeyExtractor.apply(elem.left());
      }
      return rightKeyExtractor.apply(elem.right());
    }, new HashPartitioning<>());
    this.left = left;
    this.right = right;
    this.leftKeyExtractor = leftKeyExtractor;
    this.rightKeyExtractor = rightKeyExtractor;
    this.functor = functor;
    this.output = createOutput((Dataset) left);
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


  /** Make the join outer join. */
  public Join<LEFT, RIGHT, KEY, OUT, W> outer() {
    this.outer = true;
    return this;
  }
  
  // keeper of state for window
  private class JoinState extends State<Either<LEFT, RIGHT>, Pair<KEY, OUT>> {

    // store the elements in memory for this implementation
    final Collection<LEFT> leftElements = new ArrayList<>();
    final Collection<RIGHT> rightElements = new ArrayList<>();
    final KEY key;
    final Collector<OUT> functorCollector = new Collector<OUT>() {
      @Override
      public void collect(OUT elem) {
        collector.collect(Pair.of(key, elem));
      }
    };

    public JoinState(KEY key, Collector<Pair<KEY, OUT>> collector) {
      super(collector);
      this.key = key;
    }

    @Override
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
    Map<LEFT, Either<LEFT, RIGHT>> leftMap = new Map<>(flow, left, Either::left);
    Map<RIGHT, Either<LEFT, RIGHT>> rightMap = new Map<>(flow, right, Either::right);

    Union<Either<LEFT, RIGHT>> union =
        new Union<>(flow, leftMap.output(), rightMap.output());

    ReduceStateByKey<Either<LEFT, RIGHT>, Either<LEFT, RIGHT>, Either<LEFT, RIGHT>,
        KEY, Either<LEFT, RIGHT>,
        OUT, JoinState, W> reduce;

    reduce = new ReduceStateByKey<>(
              flow,
              union.output(),
              keyExtractor::apply,
              e -> e,
              windowing,
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
              }
        );

    DAG<Operator<?, ?>> dag = DAG.of(leftMap, rightMap);
    dag.add(union, leftMap, rightMap);
    dag.add(reduce, union);
    return dag;
  }

}
