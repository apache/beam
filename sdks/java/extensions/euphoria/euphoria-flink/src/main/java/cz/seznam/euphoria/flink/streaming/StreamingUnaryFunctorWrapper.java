package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.Context;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import java.util.Objects;

public class StreamingUnaryFunctorWrapper<WID extends Window, IN, OUT>
    implements FlatMapFunction<StreamingWindowedElement<WID, IN>,
    StreamingWindowedElement<WID, OUT>>,
               ResultTypeQueryable<StreamingWindowedElement<WID, OUT>>
{
  private final UnaryFunctor<IN, OUT> f;

  public StreamingUnaryFunctorWrapper(UnaryFunctor<IN, OUT> f) {
    this.f = Objects.requireNonNull(f);
  }

  @Override
  public void flatMap(StreamingWindowedElement<WID, IN> value,
                      Collector<StreamingWindowedElement<WID, OUT>> out)
      throws Exception
  {
    f.apply(value.get(), new Context<OUT>() {
      @Override
      public void collect(OUT elem) {
        out.collect(new StreamingWindowedElement<>(value.getWindow(), elem)
            .withEmissionWatermark(value.getEmissionWatermark()));
      }
      @Override
      public Object getWindow() {
        return value.getWindow();
      }
    });
  }

  @Override
  public TypeInformation<StreamingWindowedElement<WID, OUT>> getProducedType() {
    return TypeInformation.of((Class) StreamingWindowedElement.class);
  }
}
