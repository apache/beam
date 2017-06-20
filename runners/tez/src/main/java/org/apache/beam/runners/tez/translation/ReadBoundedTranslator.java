package org.apache.beam.runners.tez.translation;

import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.Read.Bounded;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.mapreduce.input.MRInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Bounded} translation to Tez {@link DataSourceDescriptor}.
 */
class ReadBoundedTranslator<T> implements TransformTranslator<Read.Bounded<T>> {
  private static final Logger LOG = LoggerFactory.getLogger(TransformTranslator.class);

  @Override
  public void translate(Bounded<T> transform, TranslationContext context) {
    //Build datasource and add to datasource map
    DataSourceDescriptor dataSource = MRInput.createConfigBuilder(new Configuration(context.getConfig()),
        TextInputFormat.class, transform.getSource().toString()).build();
    //TODO: Support Configurable Input Formats
    context.getCurrentOutputs().forEach( (a, b) -> context.addSource(b, dataSource));
  }
}
