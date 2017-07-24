package org.apache.beam.runners.mapreduce.translation;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by peihe on 21/07/2017.
 */
public class BeamMapper<ValueInT, ValueOutT>
    extends Mapper<Object, WindowedValue<ValueInT>, Object, WindowedValue<ValueOutT>> {

  public static final String BEAM_SERIALIZED_DO_FN = "beam-serialized-do-fn";
  private static final Logger LOG = LoggerFactory.getLogger(BeamMapper.class);

  private DoFnRunner<ValueInT, ValueOutT> doFnRunner;
  private PipelineOptions options;

  @Override
  protected void setup(
      Mapper<Object, WindowedValue<ValueInT>, Object, WindowedValue<ValueOutT>>.Context context) {
    String serializedDoFn = checkNotNull(
        context.getConfiguration().get(BEAM_SERIALIZED_DO_FN),
        BEAM_SERIALIZED_DO_FN);
    doFnRunner = DoFnRunners.simpleRunner(
        options,
        (DoFn<ValueInT, ValueOutT>) SerializableUtils
            .deserializeFromByteArray(
                Base64.decodeBase64(serializedDoFn), "DoFn"),
        NullSideInputReader.empty(),
        new MROutputManager(context),
        null,
        ImmutableList.<TupleTag<?>>of(),
        null,
        WindowingStrategy.globalDefault());
  }

  @Override
  protected void map(
      Object key,
      WindowedValue<ValueInT> value,
      Mapper<Object, WindowedValue<ValueInT>, Object, WindowedValue<ValueOutT>>.Context context) {
    LOG.info("key: {}, value: {}.", key, value);
    doFnRunner.processElement(value);
  }

  @Override
  protected void cleanup(
      Mapper<Object, WindowedValue<ValueInT>, Object, WindowedValue<ValueOutT>>.Context context) {
  }

  class MROutputManager implements DoFnRunners.OutputManager {

    private final Mapper<Object, Object, Object, Object>.Context context;

    MROutputManager(Mapper<?, ?, ?, ?>.Context context) {
      this.context = (Mapper<Object, Object, Object, Object>.Context) context;
    }

    @Override
    public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
      try {
        context.write("global", output);
      } catch (Exception e) {
        Throwables.throwIfUnchecked(e);
      }
    }
  }
}
