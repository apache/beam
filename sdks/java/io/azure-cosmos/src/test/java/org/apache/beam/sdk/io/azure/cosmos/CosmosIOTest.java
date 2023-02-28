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
package org.apache.beam.sdk.io.azure.cosmos;

import static org.junit.Assert.assertEquals;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.examples.common.Families;
import com.azure.cosmos.examples.common.Family;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.azure.cosmos.CosmosIO.BoundedCosmosBDSource;
import org.apache.beam.sdk.io.azure.cosmos.CosmosIO.ConnectionConfiguration;
import org.apache.beam.sdk.io.azure.cosmos.CosmosIO.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.CosmosDBEmulatorContainer;
import org.testcontainers.utility.DockerImageName;

public class CosmosIOTest {

  private static String DOCKER_IMAGE_NAME =
      "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:latest";
  private static String DATABASE = "AzureSampleFamilyDB";
  private static String CONTAINER = "FamilyContainer";
  private static String PARTITION_KEY_PATH = "/lastName";

  private static CosmosDBEmulatorContainer container;

  private static CosmosIO.ConnectionConfiguration connectionConfiguration;

  private static CosmosClient client;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // Create the azure cosmos container.
    container = new CosmosDBEmulatorContainer(DockerImageName.parse(DOCKER_IMAGE_NAME));

    // Start the container. This step might take some time...
    container.start();
    TemporaryFolder tempFolder = new TemporaryFolder();
    tempFolder.create();
    Path keyStoreFile = tempFolder.newFile("azure-cosmos-emulator.keystore").toPath();
    KeyStore keyStore = container.buildNewKeyStore();
    keyStore.store(Files.newOutputStream(keyStoreFile), container.getEmulatorKey().toCharArray());
    System.setProperty("javax.net.ssl.trustStore", keyStoreFile.toString());
    System.setProperty("javax.net.ssl.trustStorePassword", container.getEmulatorKey());
    System.setProperty("javax.net.ssl.trustStoreType", "PKCS12");

    connectionConfiguration =
        ConnectionConfiguration.create(container.getEmulatorEndpoint())
            .withKey(container.getEmulatorKey());

    client =
        new CosmosClientBuilder()
            .gatewayMode()
            .endpointDiscoveryEnabled(false)
            .endpoint(container.getEmulatorEndpoint())
            .key(container.getEmulatorKey())
            .buildClient();

    client.createDatabase(DATABASE);
    CosmosDatabase db = client.getDatabase(DATABASE);
    db.createContainer(CONTAINER, PARTITION_KEY_PATH);
    CosmosContainer container = db.getContainer(CONTAINER);
    List<Family> families = new ArrayList<>();
    families.add(Families.getSmithFamilyItem());
    families.add(Families.getAndersenFamilyItem());
    families.add(Families.getWakefieldFamilyItem());
    families.add(Families.getJohnsonFamilyItem());

    CosmosItemRequestOptions cosmosItemRequestOptions = new CosmosItemRequestOptions();
    for (Family f : families) {
      container.createItem(f, new PartitionKey(f.getLastName()), cosmosItemRequestOptions);
    }
  }

  @AfterClass
  public static void afterClass() {
    container.stop();
    client.close();
  }

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testEstimatedSizeBytes() throws Exception {
    Read<Family> read =
        CosmosIO.read(Family.class)
            .withConnectionConfiguration(connectionConfiguration)
            .withContainer(CONTAINER)
            .withDatabase(DATABASE)
            .withCoder(SerializableCoder.of(Family.class));

    PipelineOptions options = PipelineOptionsFactory.create();
    BoundedCosmosBDSource<Family> initialSource = new BoundedCosmosBDSource<>(read);
    // CosmosDb precision is in KB. Inserted test data is ~3KB
    long estimatedSize = initialSource.getEstimatedSizeBytes(options);
    assertEquals("Wrong estimated size", 3000, estimatedSize);
  }

  @Test
  public void testRead() throws Exception {
    PCollection<Family> output =
        pipeline.apply(
            CosmosIO.read(Family.class)
                .withConnectionConfiguration(connectionConfiguration)
                .withContainer(CONTAINER)
                .withDatabase(DATABASE)
                .withCoder(SerializableCoder.of(Family.class)));

    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(4L);
    pipeline.run();
  }
}
