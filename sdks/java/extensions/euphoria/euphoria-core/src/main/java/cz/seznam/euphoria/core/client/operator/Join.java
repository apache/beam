
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.AbstractWindow;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Join two datasets by given key producing single new dataset.
 */
public class Join<IN, KEY, OUT, W extends Window<?, W>, TYPE extends Dataset<OUT>>
    extends StateAwareWindowWiseOperator<IN, KEY, OUT, W, TYPE, Join<IN, KEY, OUT, W, TYPE>> {

  public static class Builder1<IN> {
    Dataset<IN> left;
    Dataset<IN> right;
    Builder1(Dataset<IN> left, Dataset<IN> right) {
      this.left = left;
      this.right = right;
    }
    public <KEY> Builder2<IN, KEY> by(UnaryFunction<IN, KEY> extractor) {
      return new Builder2<>(left, right, extractor);
    }
  }
  public static class Builder2<IN, KEY> {
    Dataset<IN> left;
    Dataset<IN> right;
    UnaryFunction<IN, KEY> keyExtractor;
    Builder2(Dataset<IN> left, Dataset<IN> right, UnaryFunction<IN, KEY> keyExtractor) {
      this.left = left;
      this.right = right;
      this.keyExtractor = keyExtractor;
    }
    public <OUT> Builder3<IN, KEY, OUT> using(
        BinaryFunctor<IN, IN, OUT> functor) {
      return new Builder3<>(left, right, keyExtractor, functor);
    }
  }
  public static class Builder3<IN, KEY, OUT> {
    Dataset<IN> left;
    Dataset<IN> right;
    UnaryFunction<IN, KEY> keyExtractor;
    BinaryFunctor<IN, IN, OUT> joinFunc;
    Builder3(Dataset<IN> left, Dataset<IN> right,
        UnaryFunction<IN, KEY> keyExtractor,
        BinaryFunctor<IN, IN, OUT> joinFunc) {
      this.left = left;
      this.right = right;
      this.keyExtractor = keyExtractor;
      this.joinFunc = joinFunc;
    }
    public <W extends Window<?, W>> Join<IN, KEY, OUT, W, Dataset<OUT>>
    windowBy(Windowing<IN, ?, W> windowing) {
      Flow flow = left.getFlow();
      Join<IN, KEY, OUT, W, Dataset<OUT>> join = new Join<>(
          flow, left, right, windowing, keyExtractor, joinFunc);
      return flow.add(join);
    }
  }

  public static <IN> Builder1<IN> of(Dataset<IN> left, Dataset<IN> right) {
    if (right.getFlow() != left.getFlow()) {
      throw new IllegalArgumentException("Pass inputs from the same flow");
    }
    return new Builder1<>(left, right);
  }

  private final Dataset<IN> left;
  private final Dataset<IN> right;
  private final TYPE output;
  private final BinaryFunctor<IN, IN, OUT> functor;
  private boolean outer = false;

  Join(Flow flow,
      Dataset<IN> left, Dataset<IN> right,
      Windowing<IN, ?, W> windowing,
      UnaryFunction<IN, KEY> keyExtractor,
      BinaryFunctor<IN, IN, OUT> functor) {

    super("Join", flow, windowing, keyExtractor, new HashPartitioning<>());
    this.left = left;
    this.right = right;
    this.functor = functor;
    this.output = createOutput(left);
  }

  @Override
  public Collection<Dataset<IN>> listInputs() {
    return Arrays.asList(left, right);
  }

  @Override
  public TYPE output() {
    return output;
  }


  /** Make the join outer join. */
  public Join<IN, KEY, OUT, W, TYPE> outer() {
    this.outer = true;
    return this;
  }
  
  // following classes are needed by the implementation of getBasicOps
  
  private enum JOIN_FIELD {
    LEFT,
    RIGHT
  }

  private class JoinWindow
      extends AbstractWindow<Object, JoinWindow>
      implements Window<Object, JoinWindow> {

    W joinWindow;

    JoinWindow(W joinWindow) {
      this.joinWindow = joinWindow;
    }


    @Override
    public void registerTrigger(Triggering triggering,
        UnaryFunction<JoinWindow, Void> evict) {
      joinWindow.registerTrigger(triggering, w -> evict.apply(this));
    }


    @Override
    public Object getKey() {
      return joinWindow.getKey();
    }

  }

  private class JoinWindowing
      extends AbstractWindowing<Pair<JOIN_FIELD, IN>, Object, JoinWindow>
      implements Windowing<Pair<JOIN_FIELD, IN>, Object, JoinWindow> {

    @Override
    public Set<JoinWindow> allocateWindows(Pair<JOIN_FIELD, IN> what,
        Triggering triggering, UnaryFunction<JoinWindow, Void> evict) {
      // transform returned windows to joinwindows
      // FIXME: this looks strange, how to use the `evict` function correctly?
      return windowing.allocateWindows(what.getSecond(), triggering, null)
          .stream()
          .map(JoinWindow::new)
          .collect(Collectors.toSet());
    }

  }

  // keeper of state for window
  private class JoinState extends State<Pair<JOIN_FIELD, IN>, Pair<KEY, OUT>> {

    // store the elements in memory for this implementation
    final Collection<IN> leftElements = new ArrayList<>();
    final Collection<IN> rightElements = new ArrayList<>();
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
    public void add(Pair<JOIN_FIELD, IN> element) {
      if (element.getFirst() == JOIN_FIELD.LEFT) {
        leftElements.add(element.getSecond());
        emitJoinedElements(element.getSecond(), rightElements, JOIN_FIELD.LEFT);
      } else {
        rightElements.add(element.getSecond());
        emitJoinedElements(element.getSecond(), leftElements, JOIN_FIELD.RIGHT);
      }
    }


    @Override
    public void flush() {
      if (outer) {
        if (leftElements.isEmpty() ^ rightElements.isEmpty()) {
          // if just a one collection is empty
          if (leftElements.isEmpty()) {
            for (IN elem : rightElements) {
              functor.apply(null, elem, functorCollector);
            }
          } else {
            for (IN elem : leftElements) {
              functor.apply(elem, null, functorCollector);
            }
          }
        }
      }
      leftElements.clear();
      rightElements.clear();
    }

    private void emitJoinedElements(
        IN element, Collection<IN> otherElements, JOIN_FIELD type) {
      if (type == JOIN_FIELD.LEFT) {
        for (IN right : otherElements) {
          functor.apply(element, right, functorCollector);
        }
      } else {
        for (IN left : otherElements) {
          functor.apply(left, element, functorCollector);
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
    Map<IN, Pair<JOIN_FIELD, IN>, Dataset<Pair<JOIN_FIELD, IN>>> leftMap = new Map<>(
        flow, left, e -> Pair.of(JOIN_FIELD.LEFT, e));
    Map<IN, Pair<JOIN_FIELD, IN>, Dataset<Pair<JOIN_FIELD, IN>>> rightMap = new Map<>(
        flow, left, e -> Pair.of(JOIN_FIELD.RIGHT, e));

    Union<Pair<JOIN_FIELD, IN>, Dataset<Pair<JOIN_FIELD, IN>>> union = new Union<>(
        flow, leftMap.output(), rightMap.output());

    ReduceStateByKey<Pair<JOIN_FIELD, IN>, KEY, Pair<JOIN_FIELD, IN>,
            Pair<KEY, OUT>, JoinState, JoinWindow, Dataset<Pair<KEY, OUT>>> reduce;
    reduce = new ReduceStateByKey<>(
              flow,
              union.output(),
              e -> keyExtractor.apply(e.getSecond()),
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
