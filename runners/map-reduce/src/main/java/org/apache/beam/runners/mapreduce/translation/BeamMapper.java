package org.apache.beam.runners.mapreduce.translation;

import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by peihe on 21/07/2017.
 */
public class BeamMapper<KeyInT, ValueInT, KeyOutT, ValueOutT>
    extends Mapper<KeyInT, ValueInT, KeyOutT, ValueOutT> {

  private DoFnInvoker<KV<KeyInT, ValueInT>, KV<KeyOutT, ValueOutT>> doFnInvoker;

  @Override
  protected void setup(Mapper<KeyInT, ValueInT, KeyOutT, ValueOutT>.Context context) {
  }

  @Override
  protected void map(
      KeyInT key,
      ValueInT value,
      Mapper<KeyInT, ValueInT, KeyOutT, ValueOutT>.Context context) {
    System.out.print(String.format("key: %s, value: %s", key, value));
  }

  @Override
  protected void cleanup(Mapper<KeyInT, ValueInT, KeyOutT, ValueOutT>.Context context) {
  }
}
