package org.apache.beam.runners.mapreduce.translation;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 * Created by peihe on 21/07/2017.
 */
public class BeamMapper<ValueInT, ValueOutT>
    extends Mapper<Object, WindowedValue<ValueInT>, Object, WindowedValue<ValueOutT>> {

  public static final String BEAM_PAR_DO_OPERATION_MAPPER = "beam-par-do-op-mapper";

  private Operation operation;

  @Override
  protected void setup(
      Mapper<Object, WindowedValue<ValueInT>, Object, WindowedValue<ValueOutT>>.Context context) {
    String serializedParDo = checkNotNull(
        context.getConfiguration().get(BEAM_PAR_DO_OPERATION_MAPPER),
        BEAM_PAR_DO_OPERATION_MAPPER);
    operation = (Operation) SerializableUtils.deserializeFromByteArray(
        Base64.decodeBase64(serializedParDo), "Operation");
    operation.start((TaskInputOutputContext) context);
  }

  @Override
  protected void map(
      Object key,
      WindowedValue<ValueInT> value,
      Mapper<Object, WindowedValue<ValueInT>, Object, WindowedValue<ValueOutT>>.Context context) {
    operation.process(value);
  }

  @Override
  protected void cleanup(
      Mapper<Object, WindowedValue<ValueInT>, Object, WindowedValue<ValueOutT>>.Context context) {
    operation.finish();
  }
}
