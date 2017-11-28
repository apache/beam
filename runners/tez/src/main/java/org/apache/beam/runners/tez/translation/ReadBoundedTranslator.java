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
