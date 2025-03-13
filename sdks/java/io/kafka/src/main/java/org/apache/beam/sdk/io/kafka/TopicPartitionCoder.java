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
package org.apache.beam.sdk.io.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.kafka.common.TopicPartition;

/** The {@link Coder} for encoding and decoding {@link TopicPartition} in Beam. */
public class TopicPartitionCoder extends StructuredCoder<TopicPartition> {

  @Override
  public void encode(TopicPartition value, OutputStream outStream)
      throws CoderException, IOException {
    StringUtf8Coder.of().encode(value.topic(), outStream);
    VarIntCoder.of().encode(value.partition(), outStream);
  }

  @Override
  public TopicPartition decode(InputStream inStream) throws CoderException, IOException {
    String topic = StringUtf8Coder.of().decode(inStream);
    int partition = VarIntCoder.of().decode(inStream);
    return new TopicPartition(topic, partition);
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return ImmutableList.of(StringUtf8Coder.of(), VarIntCoder.of());
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {}
}
