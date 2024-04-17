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
package org.apache.beam.sdk.io.iceberg;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ExternalizableIcebergCatalogConfigTest {
  @Test
  public void testSerializeDeserialize() throws IOException, ClassNotFoundException {
    IcebergCatalogConfig config =
        IcebergCatalogConfig.builder()
            .setName("test-name")
            .setIcebergCatalogType("test-type")
            .setCatalogImplementation("test-implementation")
            .setFileIOImplementation("test-fileio")
            .setWarehouseLocation("test-location")
            .setMetricsReporterImplementation("test-metrics")
            .setCacheEnabled(true)
            .setCacheCaseSensitive(true)
            .setCacheExpirationIntervalMillis(100)
            .build();

    ExternalizableIcebergCatalogConfig externalizableConfig =
        new ExternalizableIcebergCatalogConfig(config);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);

    oos.writeObject(externalizableConfig);

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    ExternalizableIcebergCatalogConfig roundtripConfig =
        (ExternalizableIcebergCatalogConfig) ois.readObject();

    assertEquals(config, roundtripConfig.get());
  }
}
