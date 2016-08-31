package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.BinaryFunctor;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.operator.Join;
import cz.seznam.euphoria.core.client.operator.WindowedPair;
import cz.seznam.euphoria.core.client.util.Either;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.guava.shaded.com.google.common.base.Preconditions;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps
    .BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

class JoinTranslator implements StreamingOperatorTranslator<Join> {
  @Override
  public DataStream translate(FlinkOperator<Join> operator,
                              StreamingExecutorContext context)
  {
    List<DataStream<?>> inputs = context.getInputStreams(operator);
    Preconditions.checkState(inputs.size() == 2, "Join expects exactly two inputs");

    UnaryFunction leftKey = operator.getOriginalOperator().getLeftKeyExtractor();
    UnaryFunction rightKey = operator.getOriginalOperator().getRightKeyExtractor();
    BinaryFunctor joiner = operator.getOriginalOperator().getJoiner();

    DataStream<?> leftStream = inputs.get(0);
    DataStream<?> rightStream = inputs.get(1);

    if (operator.getOriginalOperator().isOuter()) {
      throw new UnsupportedOperationException("outer join not yet implemented");
    } else {
      WindowAssigner wassigner = null;
      Windowing windowing = operator.getOriginalOperator().getWindowing();
      // XXX windowing to be externalized to a cenral place
      if (windowing instanceof Time) {
        Time twindowing = (Time) windowing;
        UnaryFunction eventTimeFn = twindowing.getEventTimeFn();
        if (!(eventTimeFn instanceof Time.ProcessingTime)) {
          leftStream = leftStream.assignTimestampsAndWatermarks(
              new BoundedOutOfOrdernessTimestampExtractor(
                  org.apache.flink.streaming.api.windowing.time.Time.seconds(1)) {
                @Override
                public long extractTimestamp(Object element) {
                  return (long) ((Long) eventTimeFn.apply(Either.left(element)));
                }
              });
          rightStream = rightStream.assignTimestampsAndWatermarks(
              new BoundedOutOfOrdernessTimestampExtractor(
                  org.apache.flink.streaming.api.windowing.time.Time.seconds(1)) {
                @Override
                public long extractTimestamp(Object element) {
                  return (long) ((Long) eventTimeFn.apply(Either.right(element)));
                }
              });
          wassigner = TumblingEventTimeWindows.of(
              org.apache.flink.streaming.api.windowing.time.Time.of(
                  twindowing.getDuration(), TimeUnit.MILLISECONDS));
        } else {
          wassigner = TumblingProcessingTimeWindows.of(
              org.apache.flink.streaming.api.windowing.time.Time.of(
                  twindowing.getDuration(), TimeUnit.MILLISECONDS));
        }
      } else {
        throw new UnsupportedOperationException(
            windowing + " windowing not supported!");
      }

      DataStream output =
          leftStream.join(rightStream)
              .where(new TypedKeySelector(leftKey))
              .equalTo(new TypedKeySelector(rightKey))
              .window(wassigner)
              .apply(new Joiner(leftKey, joiner), TypeInformation.of(Object.class));
      return output;
    }
  }

  static final class Joiner implements FlatJoinFunction {
    UnaryFunction udLeftKeyExtractor;
    BinaryFunctor udJoiner;

    public Joiner(UnaryFunction udLeftKeyExtractor, BinaryFunctor udJoiner) {
      this.udLeftKeyExtractor = udLeftKeyExtractor;
      this.udJoiner = udJoiner;
    }

    @Override
    public void join(Object left, Object right, org.apache.flink.util.Collector out)
        throws Exception
    {
      Object key = udLeftKeyExtractor.apply(left);
      Collector kvc = elem -> out.collect(WindowedPair.of(null, key, elem));
      udJoiner.apply(left, right, kvc);
    }
  }

  static final class TypedKeySelector implements KeySelector, ResultTypeQueryable {
    private final UnaryFunction f;

    TypedKeySelector(UnaryFunction f) {
      this.f = Objects.requireNonNull(f);
    }

    @Override
    public Object getKey(Object value) throws Exception {
      return f.apply(value);
    }

    @Override
    public TypeInformation getProducedType() {
      return TypeInformation.of(Object.class);
    }
  }
}
