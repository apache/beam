package cz.seznam.euphoria.flink.translation.functions;

import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

public class UnaryFunctorWrapper<IN, OUT>
        implements FlatMapFunction<IN, OUT>, ResultTypeQueryable<OUT>
{
  private final UnaryFunctor<IN, OUT> functor;

  public UnaryFunctorWrapper(UnaryFunctor<IN, OUT> functor) {
    this.functor = functor;
  }

  @Override
  public void flatMap(IN elem, Collector<OUT> collector) throws Exception {
    functor.apply(elem, collector::collect);
  }

  @Override
  @SuppressWarnings("unchecked")
  public TypeInformation<OUT> getProducedType() {
    return TypeInformation.of((Class) Object.class);
  }
}
