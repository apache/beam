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
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.azure.cosmos.CosmosIO.BoundedCosmosBDSource;
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

// See https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/quickstart-java
public class CosmosIOTest {

  private static final String DOCKER_IMAGE_NAME =
      "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:latest";
  private static final String DATABASE = "AzureSampleFamilyDB";
  private static final String CONTAINER = "FamilyContainer";
  private static final String PARTITION_KEY_PATH = "/lastName";

  private static PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
  private static CosmosDBEmulatorContainer container;
  private static CosmosClient client;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // Create the azure cosmos container and pull image as we use latest
    container = new CosmosDBEmulatorContainer(DockerImageName.parse(DOCKER_IMAGE_NAME));
    container.withImagePullPolicy(imageName -> true);

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

    CosmosOptions cosmosOptions = pipelineOptions.as(CosmosOptions.class);
    cosmosOptions.setCosmosServiceEndpoint(container.getEmulatorEndpoint());
    cosmosOptions.setCosmosKey(container.getEmulatorKey());

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
    families.add(Families.getAndersenFamilyItem());
    families.add(Families.getJohnsonFamilyItem());
    families.add(Families.getSmithFamilyItem());
    families.add(Families.getWakefieldFamilyItem());

    CosmosItemRequestOptions cosmosItemRequestOptions = new CosmosItemRequestOptions();
    for (Family f : families) {
      container.createItem(f, new PartitionKey(f.getLastName()), cosmosItemRequestOptions);
    }
  }

  @AfterClass
  public static void afterClass() {
    container.stop();
    if (client != null) {
      client.close();
    }
  }

  @Rule public TestPipeline pipeline = TestPipeline.fromOptions(pipelineOptions);

  @Test
  public void testEstimatedSizeBytes() throws Exception {
    Read<Family> read =
        CosmosIO.read(Family.class)
            .withContainer(CONTAINER)
            .withDatabase(DATABASE)
            .withCoder(SerializableCoder.of(Family.class));

    BoundedCosmosBDSource<Family> initialSource = new BoundedCosmosBDSource<>(read);
    // Cosmos DB precision is in KB. Inserted test data is ~3KB
    long estimatedSize = initialSource.getEstimatedSizeBytes(pipelineOptions);
    assertEquals("Wrong estimated size", 3072, estimatedSize);
  }

  @Test
  public void testSplit() throws Exception {
    Read<Family> read =
        CosmosIO.read(Family.class)
            .withContainer(CONTAINER)
            .withDatabase(DATABASE)
            .withCoder(SerializableCoder.of(Family.class));

    BoundedCosmosBDSource<Family> initialSource = new BoundedCosmosBDSource<>(read);
    // Cosmos DB precision is in KB. Inserted test data is ~3KB
    List<? extends BoundedSource<Family>> splits = initialSource.split(1024, pipelineOptions);
    assertEquals("Wrong split", 3, splits.size());
  }

  @Test
  public void testRead() {
    PCollection<Family> output =
        pipeline.apply(
            CosmosIO.read(Family.class)
                .withContainer(CONTAINER)
                .withDatabase(DATABASE)
                .withCoder(SerializableCoder.of(Family.class)));

    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(4L);
    pipeline.run();
  }

  @Test
  public void testReadWithQuery() {
    PCollection<Family> output =
        pipeline.apply(
            CosmosIO.read(Family.class)
                .withContainer(CONTAINER)
                .withDatabase(DATABASE)
                .withCoder(SerializableCoder.of(Family.class))
                .withQuery(
                    "SELECT * FROM Family WHERE Family.lastName IN ('Andersen', 'Wakefield', 'Johnson')"));

    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(3L);
    pipeline.run();
  }
}
