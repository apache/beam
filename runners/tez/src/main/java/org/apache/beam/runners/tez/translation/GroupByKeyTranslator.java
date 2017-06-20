package org.apache.beam.runners.tez.translation;

import com.google.common.collect.Iterables;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.values.PValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link GroupByKey} translation to Tez {@link org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig}
 */
class GroupByKeyTranslator<K, V> implements TransformTranslator<GroupByKey<K, V>> {
  private static final Logger LOG = LoggerFactory.getLogger(GroupByKey.class);

  @Override
  public void translate(GroupByKey<K, V> transform, TranslationContext context) {
    if (context.getCurrentInputs().size() > 1 ){
      throw new RuntimeException("Multiple Inputs are not yet supported");
    } else if (context.getCurrentOutputs().size() > 1){
      throw new RuntimeException("Multiple Outputs are not yet supported");
    }
    PValue input = Iterables.getOnlyElement(context.getCurrentInputs().values());
    PValue output = Iterables.getOnlyElement(context.getCurrentOutputs().values());
    context.addShufflePair(input, output);
  }
}