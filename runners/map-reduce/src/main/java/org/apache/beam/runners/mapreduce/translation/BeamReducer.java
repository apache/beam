package org.apache.beam.runners.mapreduce.translation;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 * Created by peihe on 25/07/2017.
 */
public class BeamReducer<ValueInT, ValueOutT>
    extends Reducer<Object, byte[], Object, WindowedValue<ValueOutT>> {

  public static final String BEAM_PAR_DO_OPERATION_REDUCER = "beam-par-do-op-reducer";

  private ParDoOperation parDoOperation;

  @Override
  protected void setup(
      Reducer<Object, byte[], Object, WindowedValue<ValueOutT>>.Context context) {
    String serializedParDo = checkNotNull(
        context.getConfiguration().get(BEAM_PAR_DO_OPERATION_REDUCER),
        BEAM_PAR_DO_OPERATION_REDUCER);
    parDoOperation = (ParDoOperation) SerializableUtils.deserializeFromByteArray(
        Base64.decodeBase64(serializedParDo), "ParDoOperation");
    parDoOperation.start((TaskInputOutputContext) context);
  }

  @Override
  protected void reduce(
      Object key,
      Iterable<byte[]> values,
      Reducer<Object, byte[], Object, WindowedValue<ValueOutT>>.Context context) {
    Iterable<Object> decodedValues = FluentIterable.from(values)
        .transform(new Function<byte[], Object>() {
          @Override
          public Object apply(byte[] input) {
            ByteArrayInputStream inStream = new ByteArrayInputStream(input);
            try {
              // TODO: setup coders.
              return NullableCoder.of(BigEndianLongCoder.of()).decode(inStream);
            } catch (IOException e) {
              Throwables.throwIfUnchecked(e);
              throw new RuntimeException(e);
            }
          }
        });
    parDoOperation.process(
        WindowedValue.valueInGlobalWindow(KV.of(key, decodedValues)));
  }

  @Override
  protected void cleanup(
      Reducer<Object, byte[], Object, WindowedValue<ValueOutT>>.Context context) {
    parDoOperation.finish();
  }
}
