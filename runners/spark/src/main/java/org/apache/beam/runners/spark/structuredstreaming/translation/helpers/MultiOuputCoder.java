package org.apache.beam.runners.spark.structuredstreaming.translation.helpers;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import scala.Tuple2;

public class MultiOuputCoder<T> extends CustomCoder<Tuple2<TupleTag<T>, WindowedValue<T>>> {
  Coder<TupleTag> tupleTagCoder;
  Map<TupleTag<?>, Coder<?>> coderMap;
  Coder<? extends BoundedWindow> windowCoder;

  public static MultiOuputCoder of(Coder<TupleTag> tupleTagCoder, Map<TupleTag<?>, Coder<?>> coderMap, Coder<? extends BoundedWindow> windowCoder) {
    return new MultiOuputCoder(tupleTagCoder, coderMap, windowCoder);
  }

  private MultiOuputCoder(Coder<TupleTag> tupleTagCoder, Map<TupleTag<?>, Coder<?>> coderMap, Coder<? extends BoundedWindow> windowCoder) {
    this.tupleTagCoder = tupleTagCoder;
    this.coderMap = coderMap;
    this.windowCoder = windowCoder;
  }

  @Override public void encode(Tuple2<TupleTag<T>, WindowedValue<T>> tuple2, OutputStream outStream)
      throws IOException {
    TupleTag<T> tupleTag = tuple2._1();
    tupleTagCoder.encode(tupleTag, outStream);
    Coder<T> valueCoder = (Coder<T>)coderMap.get(tupleTag);
    WindowedValue.FullWindowedValueCoder<T> wvCoder = WindowedValue.FullWindowedValueCoder
        .of(valueCoder, windowCoder);
    wvCoder.encode(tuple2._2(), outStream);
  }

  @Override public Tuple2<TupleTag<T>, WindowedValue<T>> decode(InputStream inStream)
      throws CoderException, IOException {
    TupleTag<T> tupleTag = (TupleTag<T>) tupleTagCoder.decode(inStream);
    Coder<T> valueCoder = (Coder<T>)coderMap.get(tupleTag);
    WindowedValue.FullWindowedValueCoder<T> wvCoder = WindowedValue.FullWindowedValueCoder
        .of(valueCoder, windowCoder);
    WindowedValue<T> wv = wvCoder.decode(inStream);
    return Tuple2.apply(tupleTag, wv);
  }
}
