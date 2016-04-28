
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.dataset.AbstractWindowing;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.HashPartitioning;
import cz.seznam.euphoria.core.client.dataset.Window;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.BinaryFunctor;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.dataset.Triggering;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.util.Either;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Join two datasets by given key producing single new dataset.
 */
public class Join<LEFT, RIGHT, KEY, OUT, W extends Window<?>, TYPE extends Dataset<Pair<KEY, OUT>>>
    extends StateAwareWindowWiseOperator<Object, Either<LEFT, RIGHT>, Either<LEFT, RIGHT>, KEY, Pair<KEY, OUT>, W, TYPE,
    Join<LEFT, RIGHT, KEY, OUT, W, TYPE>> {

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
    public <W extends Window<?>> Join<LEFT, RIGHT, KEY, OUT, W, Dataset<Pair<KEY, OUT>>>
    windowBy(Windowing<Either<LEFT, RIGHT>, ?, W> windowing) {
      Flow flow = left.getFlow();
      Join<LEFT, RIGHT, KEY, OUT, W, Dataset<Pair<KEY, OUT>>> join = new Join<>(
          flow, left, right, windowing, leftKeyExtractor, rightKeyExtractor, joinFunc);
      return flow.add(join);
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
  private final TYPE output;
  private final BinaryFunctor<LEFT, RIGHT, OUT> functor;
  final UnaryFunction<LEFT, KEY> leftKeyExtractor;
  final UnaryFunction<RIGHT, KEY> rightKeyExtractor;

  private boolean outer = false;

  @SuppressWarnings("unchecked")
  Join(Flow flow,
      Dataset<LEFT> left, Dataset<RIGHT> right,
      Windowing<Either<LEFT, RIGHT>, ?, W> windowing,
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
    this.output = (TYPE) createOutput((Dataset) left);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<Dataset<Object>> listInputs() {
    return Arrays.asList((Dataset) left, (Dataset) right);
  }

  @Override
  public TYPE output() {
    return output;
  }


  /** Make the join outer join. */
  public Join<LEFT, RIGHT, KEY, OUT, W, TYPE> outer() {
    this.outer = true;
    return this;
  }
  
  // following classes are needed by the implementation of getBasicOps
  
  private class JoinWindow implements Window<Object> {

    W joinWindow;

    JoinWindow(W joinWindow) {
      this.joinWindow = joinWindow;
    }


    @Override
    public void registerTrigger(Triggering triggering,
        UnaryFunction<Window<?>, Void> evict) {
      joinWindow.registerTrigger(triggering, w -> evict.apply(this));
    }


    @Override
    public Object getKey() {
      return joinWindow.getKey();
    }

    @Override
    public Set<State<?, ?>> getStates() {
      return joinWindow.getStates();
    }

    @Override
    public void addState(State<?, ?> state) {
      joinWindow.addState(state);
    }

  }

  private class JoinWindowing
      extends AbstractWindowing<Either<LEFT, RIGHT>, Object, JoinWindow>
      implements Windowing<Either<LEFT, RIGHT>, Object, JoinWindow> {

    final java.util.Map<W, JoinWindow> allocatedWindows = new HashMap<>();

    @Override
    public Set<JoinWindow> allocateWindows(Either<LEFT, RIGHT> what,
        Triggering triggering, UnaryFunction<Window<?>, Void> evict) {
      // transform returned windows to joinwindows
      Set<W> windows = windowing.allocateWindows(what, triggering, evict);
      return windows.stream()
          .map(w -> {
            JoinWindow allocated = allocatedWindows.get(w);
            if (allocated == null) {
              allocatedWindows.put(w, allocated = new JoinWindow(w));
            }
            return allocated;
          })
          .collect(Collectors.toSet());
    }

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
      if (outer) {
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
      leftElements.clear();
      rightElements.clear();
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
        leftElements.addAll(state.leftElements);
        rightElements.addAll(state.rightElements);
      }
      return this;
    }

  }

  @Override
  @SuppressWarnings("unchecked")
  public DAG<Operator<?, ?, ?>> getBasicOps() {


    Flow flow = getFlow();
    Map<LEFT, Either<LEFT, RIGHT>, Dataset<Either<LEFT, RIGHT>>> leftMap = new Map<>(
        flow, left, (LEFT e) -> Either.left(e));
    Map<RIGHT, Either<LEFT, RIGHT>, Dataset<Either<LEFT, RIGHT>>> rightMap = new Map<>(
        flow, right, (RIGHT e) -> Either.right(e));

    Union<Either<LEFT, RIGHT>, Dataset<Either<LEFT, RIGHT>>> union = new Union<>(
        flow, leftMap.output(), rightMap.output());

    ReduceStateByKey<Either<LEFT, RIGHT>, Either<LEFT, RIGHT>, Either<LEFT, RIGHT>,
        KEY, Either<LEFT, RIGHT>,
        OUT, JoinState, JoinWindow, Dataset<Pair<KEY, OUT>>> reduce;

    reduce = new ReduceStateByKey<>(
              flow,
              union.output(),
              e -> keyExtractor.apply(e),
              e -> e,
              new JoinWindowing(),
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

    DAG<Operator<?, ?, ?>> dag = DAG.of(leftMap, rightMap);
    dag.add(union, leftMap, rightMap);
    dag.add(reduce, union);
    return dag;
  }

}
