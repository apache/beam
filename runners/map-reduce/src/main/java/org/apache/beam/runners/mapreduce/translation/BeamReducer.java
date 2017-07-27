package org.apache.beam.runners.mapreduce.translation;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 * Created by peihe on 25/07/2017.
 */
public class BeamReducer<ValueInT, ValueOutT>
    extends Reducer<BytesWritable, byte[], Object, WindowedValue<ValueOutT>> {

  public static final String BEAM_REDUCER_KV_CODER = "beam-reducer-kv-coder";
  public static final String BEAM_PAR_DO_OPERATION_REDUCER = "beam-par-do-op-reducer";

  private Coder<Object> keyCoder;
  private Coder<Object> valueCoder;
  private Operation operation;

  @Override
  protected void setup(
      Reducer<BytesWritable, byte[], Object, WindowedValue<ValueOutT>>.Context context) {
    String serializedValueCoder = checkNotNull(
        context.getConfiguration().get(BEAM_REDUCER_KV_CODER),
        BEAM_REDUCER_KV_CODER);
    String serializedParDo = checkNotNull(
        context.getConfiguration().get(BEAM_PAR_DO_OPERATION_REDUCER),
        BEAM_PAR_DO_OPERATION_REDUCER);
    KvCoder<Object, Object> kvCoder = (KvCoder<Object, Object>) SerializableUtils
        .deserializeFromByteArray(Base64.decodeBase64(serializedValueCoder), "Coder");
    keyCoder = kvCoder.getKeyCoder();
    valueCoder = kvCoder.getValueCoder();
    operation = (Operation) SerializableUtils.deserializeFromByteArray(
        Base64.decodeBase64(serializedParDo), "Operation");
    operation.start((TaskInputOutputContext) context);
  }

  @Override
  protected void reduce(
      BytesWritable key,
      Iterable<byte[]> values,
      Reducer<BytesWritable, byte[], Object, WindowedValue<ValueOutT>>.Context context) {
    List<Object> decodedValues = Lists.newArrayList(FluentIterable.from(values)
        .transform(new Function<byte[], Object>() {
          @Override
          public Object apply(byte[] input) {
            ByteArrayInputStream inStream = new ByteArrayInputStream(input);
            try {
              return valueCoder.decode(inStream);
            } catch (IOException e) {
              Throwables.throwIfUnchecked(e);
              throw new RuntimeException(e);
            }
          }}));

    try {
      operation.process(
          WindowedValue.valueInGlobalWindow(
              KV.of(keyCoder.decode(new ByteArrayInputStream(key.getBytes())), decodedValues)));
    } catch (IOException e) {
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void cleanup(
      Reducer<BytesWritable, byte[], Object, WindowedValue<ValueOutT>>.Context context) {
    operation.finish();
  }
}
