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
package org.apache.beam.sdk.io.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.beam.repackaged.core.org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for SerializableConfiguration. */
@RunWith(JUnit4.class)
public class SerializableConfigurationTest {
  @Rule public final ExpectedException thrown = ExpectedException.none();
  private static final SerializableConfiguration DEFAULT_SERIALIZABLE_CONF =
      new SerializableConfiguration(new Configuration());

  @Test
  public void testSerializationDeserialization() {
    Configuration conf = new Configuration();
    conf.set("hadoop.silly.test", "test-value");
    byte[] object = SerializationUtils.serialize(new SerializableConfiguration(conf));
    SerializableConfiguration serConf = SerializationUtils.deserialize(object);
    assertNotNull(serConf);
    assertEquals("test-value", serConf.get().get("hadoop.silly.test"));
  }

  @Test
  public void testConstruction() {
    assertNotNull(DEFAULT_SERIALIZABLE_CONF);
    assertNotNull(DEFAULT_SERIALIZABLE_CONF.get());
    thrown.expect(NullPointerException.class);
    new SerializableConfiguration(null);
  }

  @Test
  public void testCreateNewConfiguration() {
    Configuration confFromNull = SerializableConfiguration.newConfiguration(null);
    assertNotNull(confFromNull);
    Configuration conf =
        SerializableConfiguration.newConfiguration(new SerializableConfiguration(confFromNull));
    assertNotNull(conf);
  }

  @Test
  public void testCreateNewJob() throws Exception {
    Job jobFromNull = SerializableConfiguration.newJob(null);
    assertNotNull(jobFromNull);
    Job job = SerializableConfiguration.newJob(DEFAULT_SERIALIZABLE_CONF);
    assertNotNull(job);
  }
}
