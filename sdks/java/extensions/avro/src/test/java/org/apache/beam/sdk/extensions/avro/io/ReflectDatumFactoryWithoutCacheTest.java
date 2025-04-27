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
package org.apache.beam.sdk.extensions.avro.io;

import org.apache.avro.Schema;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ReflectDatumFactoryWithoutCacheTest {

  @Test
  public void whenCacheIsDisabledThenAvroReaderAndWriterAreCreatedEachTime() {
    System.setProperty("beam.avro.reflectdatumfactory.cachesize", "-1"); // disable cache

    AvroDatumFactory.ReflectDatumFactory<MyClass> factory =
        new AvroDatumFactory.ReflectDatumFactory<>(MyClass.class);
    Schema schema = AvroCoder.of(ReflectDatumFactoryWithCacheTest.MyClass.class).getSchema();

    Assert.assertNotSame(factory.apply(schema), factory.apply(schema));
    Assert.assertNotSame(factory.apply(schema, schema), factory.apply(schema, schema));
  }

  static class MyClass {
    public String field1;
  }
}
