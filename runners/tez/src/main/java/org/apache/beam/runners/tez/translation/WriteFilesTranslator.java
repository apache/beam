/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
