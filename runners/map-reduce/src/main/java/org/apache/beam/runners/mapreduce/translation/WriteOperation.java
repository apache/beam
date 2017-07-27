package org.apache.beam.runners.mapreduce.translation;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Throwables;
import java.io.ByteArrayOutputStream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 * Created by peihe on 26/07/2017.
 */
public class WriteOperation extends Operation {

  private final Coder<Object> keyCoder;
  private final Coder<Object> valueCoder;

  private transient TaskInputOutputContext<Object, Object, Object, Object> taskContext;

  public WriteOperation(Coder<Object> keyCoder, Coder<Object> valueCoder) {
    super(0);
    this.keyCoder = checkNotNull(keyCoder, "keyCoder");
    this.valueCoder = checkNotNull(valueCoder, "valueCoder");
  }

  @Override
  public void start(TaskInputOutputContext<Object, Object, Object, Object> taskContext) {
    this.taskContext = checkNotNull(taskContext, "taskContext");
  }

  @Override
  public void process(Object elem) {
    WindowedValue<KV<?, ?>> windowedElem = (WindowedValue<KV<?, ?>>) elem;
    try {
      ByteArrayOutputStream keyStream = new ByteArrayOutputStream();
      keyCoder.encode(windowedElem.getValue().getKey(), keyStream);

      ByteArrayOutputStream valueStream = new ByteArrayOutputStream();
      valueCoder.encode(windowedElem.getValue().getValue(), valueStream);
      taskContext.write(new BytesWritable(keyStream.toByteArray()), valueStream.toByteArray());
    } catch (Exception e) {
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }
}
