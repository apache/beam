package cz.seznam.euphoria.flink.functions;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.Context;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import java.util.Objects;

public class UnaryFunctorWrapper<LABEL, IN, OUT>
    implements FlatMapFunction<WindowedElement<LABEL, IN>,
                               WindowedElement<LABEL, OUT>>,
               ResultTypeQueryable<WindowedElement<LABEL, OUT>>
{
  private final UnaryFunctor<IN, OUT> f;

  public UnaryFunctorWrapper(UnaryFunctor<IN, OUT> f) {
    this.f = Objects.requireNonNull(f);
  }

  @Override
  public void flatMap(WindowedElement<LABEL, IN> value,
                      Collector<WindowedElement<LABEL, OUT>> out)
      throws Exception
  {
    f.apply(value.get(), new Context<OUT>() {
      @Override
      public void collect(OUT elem) {
        out.collect(new WindowedElement<>(value.getWindowID(), elem));
      }
      @Override
      public Object getWindow() {
        return value.getWindowID().getLabel();
      }
    });
  }

  @Override
  public TypeInformation<WindowedElement<LABEL, OUT>> getProducedType() {
    return TypeInformation.of((Class) WindowedElement.class);
  }
}
