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
package org.apache.beam.sdk.extensions.avro.coders;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DefaultCoderTestAvro {
  @DefaultCoder(AvroCoder.class)
  private static class AvroRecord {}

  @Test
  public void testCodersWithoutComponents() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    registry.registerCoderProvider(
        new DefaultCoder.DefaultCoderProviderRegistrar.DefaultCoderProvider());
    assertThat(registry.getCoder(AvroRecord.class), instanceOf(AvroCoder.class));
  }

  @Test
  public void testDefaultCoderInCollection() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    registry.registerCoderProvider(
        new DefaultCoder.DefaultCoderProviderRegistrar.DefaultCoderProvider());
    Coder<List<AvroRecord>> avroRecordCoder =
        registry.getCoder(new TypeDescriptor<List<AvroRecord>>() {});
    assertThat(avroRecordCoder, instanceOf(ListCoder.class));
    assertThat(((ListCoder) avroRecordCoder).getElemCoder(), instanceOf(AvroCoder.class));
  }
}
