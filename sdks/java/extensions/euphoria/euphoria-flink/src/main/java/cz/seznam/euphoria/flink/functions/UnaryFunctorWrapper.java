package cz.seznam.euphoria.flink.functions;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import java.util.Objects;

public class UnaryFunctorWrapper<GROUP, LABEL, IN, OUT>
    implements FlatMapFunction<WindowedElement<GROUP, LABEL, IN>,
                               WindowedElement<GROUP, LABEL, OUT>>,
               ResultTypeQueryable<WindowedElement<GROUP, LABEL, OUT>>
{
  private final UnaryFunctor<IN, OUT> f;

  public UnaryFunctorWrapper(UnaryFunctor<IN, OUT> f) {
    this.f = Objects.requireNonNull(f);
  }

  @Override
  public void flatMap(WindowedElement<GROUP, LABEL, IN> value,
                      Collector<WindowedElement<GROUP, LABEL, OUT>> out)
      throws Exception
  {
    f.apply(value.get(), elem -> {
      out.collect(new WindowedElement(value.getWindowID(), elem));
    });
  }

  @Override
  public TypeInformation<WindowedElement<GROUP, LABEL, OUT>> getProducedType() {
    return TypeInformation.of((Class) WindowedElement.class);
  }
}
