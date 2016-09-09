package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import java.util.Objects;

public class StreamingUnaryFunctorWrapper<GROUP, LABEL, IN, OUT>
    implements FlatMapFunction<StreamingWindowedElement<GROUP, LABEL, IN>,
    StreamingWindowedElement<GROUP, LABEL, OUT>>,
               ResultTypeQueryable<StreamingWindowedElement<GROUP, LABEL, OUT>>
{
  private final UnaryFunctor<IN, OUT> f;

  public StreamingUnaryFunctorWrapper(UnaryFunctor<IN, OUT> f) {
    this.f = Objects.requireNonNull(f);
  }

  @Override
  public void flatMap(StreamingWindowedElement<GROUP, LABEL, IN> value,
                      Collector<StreamingWindowedElement<GROUP, LABEL, OUT>> out)
      throws Exception
  {
    f.apply(value.get(), elem -> {
      out.collect(new StreamingWindowedElement<>(value.getWindowID(), elem)
                      .withEmissionWatermark(value.getEmissionWatermark()));
    });
  }

  @Override
  public TypeInformation<StreamingWindowedElement<GROUP, LABEL, OUT>> getProducedType() {
    return TypeInformation.of((Class) StreamingWindowedElement.class);
  }
}
