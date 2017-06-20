package org.apache.beam.runners.tez.translation;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.mapreduce.output.MROutput;

/**
 * {@link MROutput} translation to Tez {@link DataSinkDescriptor}.
 */
class WriteFilesTranslator implements TransformTranslator<WriteFiles<?>> {

  @Override
  public void translate(WriteFiles transform, TranslationContext context) {
    Pattern pattern = Pattern.compile(".*\\{.*\\{value=(.*)}}.*");
    Matcher matcher = pattern.matcher(transform.getSink().getBaseOutputDirectoryProvider().toString());
    if (matcher.matches()){
      String output = matcher.group(1);
      DataSinkDescriptor dataSink = MROutput.createConfigBuilder(new Configuration(context.getConfig()),
          TextOutputFormat.class, output).build();

      context.getCurrentInputs().forEach( (a, b) -> context.addSink(b, dataSink));
    }
  }
}
