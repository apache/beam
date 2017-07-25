package org.apache.beam.runners.mapreduce.translation;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by peihe on 21/07/2017.
 */
public class BeamMapper<ValueInT, ValueOutT>
    extends Mapper<Object, WindowedValue<ValueInT>, Object, WindowedValue<ValueOutT>> {

  public static final String BEAM_SERIALIZED_PAR_DO_OPERATION = "beam-serialized-par-do-op";

  private ParDoOperation parDoOperation;

  @Override
  protected void setup(
      Mapper<Object, WindowedValue<ValueInT>, Object, WindowedValue<ValueOutT>>.Context context) {
    String serializedParDo = checkNotNull(
        context.getConfiguration().get(BEAM_SERIALIZED_PAR_DO_OPERATION),
        BEAM_SERIALIZED_PAR_DO_OPERATION);
    parDoOperation = (ParDoOperation) SerializableUtils.deserializeFromByteArray(
        Base64.decodeBase64(serializedParDo), "DoFn");
    parDoOperation.start();
  }

  @Override
  protected void map(
      Object key,
      WindowedValue<ValueInT> value,
      Mapper<Object, WindowedValue<ValueInT>, Object, WindowedValue<ValueOutT>>.Context context) {
    parDoOperation.process(value);
  }

  @Override
  protected void cleanup(
      Mapper<Object, WindowedValue<ValueInT>, Object, WindowedValue<ValueOutT>>.Context context) {
    parDoOperation.finish();
  }
}
