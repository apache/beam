package org.apache.beam.runners.spark.io;

import static org.apache.beam.sdk.transforms.windowing.BoundedWindow.TIMESTAMP_MIN_VALUE;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder.SparkWatermarks;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.TestStream.ElementEvent;
import org.apache.beam.sdk.testing.TestStream.ProcessingTimeEvent;
import org.apache.beam.sdk.testing.TestStream.WatermarkEvent;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaSparkContext$;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.dstream.InputDStream;
import org.joda.time.Instant;
import scala.Option;
import scala.reflect.ClassTag;

public class TestDStream<T> extends InputDStream<WindowedValue<T>> {
  private final Coder<WindowedValue<T>> coder;
  private final transient Iterator<TestStream.Event<T>> events;
  private @Nullable SparkWatermarks watermarks = null;
  private boolean stopped = false;

  public TestDStream(TestStream<T> testStream, StreamingContext ssc) {
    super(ssc, classTag());
    this.coder =
        WindowedValue.getFullCoder(testStream.getValueCoder(), GlobalWindow.Coder.INSTANCE);
    this.events = testStream.getEvents().iterator();
  }

  @Override
  public Option<RDD<WindowedValue<T>>> compute(Time validTime) {
    Preconditions.checkStateNotNull(events, "Events iterator null");

    if (!events.hasNext()) {
      return Option.apply(emptyRDD());
    }

    TestStream.Event<T> event = events.next();
    if (event instanceof ElementEvent) {
      // sync watermarks if required
      while (!stopped
          && watermarks != null
          && watermarkOutOfSync(watermarks.getSynchronizedProcessingTime())) {
        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
      }
      return Option.apply(buildRdd((ElementEvent<T>) event));
    } else if (event instanceof WatermarkEvent) {
      Instant newLow = watermarks != null ? watermarks.getHighWatermark() : TIMESTAMP_MIN_VALUE;
      Instant newHigh = ((WatermarkEvent<T>) event).getWatermark();
      watermarks = new SparkWatermarks(newLow, newHigh, new Instant(validTime.milliseconds()));
      GlobalWatermarkHolder.add(id(), watermarks);
      return Option.apply(emptyRDD());
    } else if (event instanceof ProcessingTimeEvent) {
      throw new UnsupportedOperationException(
          "Advancing Processing time is not supported by the Spark Runner.");
    } else {
      return Option.apply(emptyRDD());
    }
  }

  private boolean watermarkOutOfSync(Instant time) {
    return GlobalWatermarkHolder.getLastWatermarkedBatchTime() < time.getMillis();
  }

  @Override
  public void start() {}

  @Override
  public void stop() {
    stopped = true;
  }

  private RDD<WindowedValue<T>> emptyRDD() {
    return ssc().sparkContext().emptyRDD(classTag());
  }

  private RDD<WindowedValue<T>> buildRdd(ElementEvent<T> event) {
    List<byte[]> binaryData = new ArrayList<>();
    for (TimestampedValue<T> elem : event.getElements()) {
      WindowedValue<T> wv =
          WindowedValue.timestampedValueInGlobalWindow(elem.getValue(), elem.getTimestamp());
      binaryData.add(CoderHelpers.toByteArray(wv, coder));
    }

    return new JavaSparkContext(ssc().sparkContext())
        .parallelize(binaryData)
        .map(CoderHelpers.fromByteFunction(coder))
        .rdd();
  }

  private static final <T> ClassTag<WindowedValue<T>> classTag() {
    return JavaSparkContext$.MODULE$.fakeClassTag();
  }
}
