package cz.seznam.euphoria.spark;

import com.google.common.collect.Iterators;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Iterator;
import java.util.Objects;

class UnaryFunctorWrapper<WID extends Window, IN, OUT>
        implements FlatMapFunction<WindowedElement<WID, IN>, WindowedElement<WID, OUT>> {

  private final FunctionContextMem<OUT> context;
  private final UnaryFunctor<IN, OUT> functor;

  public UnaryFunctorWrapper(UnaryFunctor<IN, OUT> functor) {
    this.functor = Objects.requireNonNull(functor);
    this.context = new FunctionContextMem<>();
  }

  @Override
  public Iterator<WindowedElement<WID, OUT>> call(WindowedElement<WID, IN> elem) {
    final WID window = elem.getWindow();
    final long timestamp = elem.getTimestamp();

    // setup user context
    context.clear();
    context.setWindow(window);

    functor.apply(elem.getElement(), context);

    // wrap output in WindowedElement
    return Iterators.transform(context.getOutputIterator(),
            e -> new WindowedElement<>(window, timestamp, e));
  }
}
