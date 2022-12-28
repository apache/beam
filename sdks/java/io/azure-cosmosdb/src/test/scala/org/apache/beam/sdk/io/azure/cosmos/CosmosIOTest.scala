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
package org.apache.beam.sdk.io.azure.cosmos

import com.azure.cosmos.CosmosClientBuilder
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.azure.cosmos.CosmosIOTest.{ CONTAINER, DATABASE, cosmosDBEmulatorContainer }
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Count
import org.apache.beam.sdk.values.PCollection
import org.bson.Document
import org.junit._
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.testcontainers.containers.CosmosDBEmulatorContainer
import org.testcontainers.utility.DockerImageName
import scribe._

import java.nio.file.Files
import scala.util.Using

@RunWith(classOf[JUnit4])
class CosmosIOTest {
  Logger.root
    .clearHandlers()
    .clearModifiers()
    .withHandler(
      minimumLevel = Some(Level.Debug),
    )
    .replace()

  @Test
  def readFromCosmosCoreSqlApi(): Unit = {
    val read = CosmosIO
      .read()
      .withCosmosEndpoint(cosmosDBEmulatorContainer.getEmulatorEndpoint)
      .withCosmosKey(cosmosDBEmulatorContainer.getEmulatorKey)
      .withQuery(s"SELECT * FROM c")
      .withContainer(CONTAINER)
      .withDatabase(DATABASE)

    val pipeline = Pipeline.create()
    val count: PCollection[java.lang.Long] = pipeline
      .apply(read)
      .apply(Count.globally())

    PAssert.thatSingleton(count).isEqualTo(10)

    pipeline.run().waitUntilFinish()
  }
}

/** Initialization of static fields and methods */
@RunWith(classOf[JUnit4])
object CosmosIOTest {
  Logger.root
    .clearHandlers()
    .clearModifiers()
    .withHandler(
      minimumLevel = Some(Level.Debug),
    )
    .replace()
  private val DOCKER_NAME = "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:latest"
  private val cosmosDBEmulatorContainer = new CosmosDBEmulatorContainer(DockerImageName.parse(DOCKER_NAME))
  private val DATABASE = "test"
  private val CONTAINER = "test"

  @BeforeClass
  def setup(): Unit = {
    info("Starting CosmosDB emulator")
    cosmosDBEmulatorContainer.start()

    val tempFolder = new TemporaryFolder
    tempFolder.create()
    val keyStoreFile = tempFolder.newFile("azure-cosmos-emulator.keystore").toPath
    val keyStore = cosmosDBEmulatorContainer.buildNewKeyStore
    keyStore.store(Files.newOutputStream(keyStoreFile.toFile.toPath), cosmosDBEmulatorContainer.getEmulatorKey.toCharArray)
    System.setProperty("javax.net.ssl.trustStore", keyStoreFile.toString)
    System.setProperty("javax.net.ssl.trustStorePassword", cosmosDBEmulatorContainer.getEmulatorKey)
    System.setProperty("javax.net.ssl.trustStoreType", "PKCS12")


    info("Creando la data -------------------------------------------------------->")
    val triedCreateData = Using(new CosmosClientBuilder()
      .gatewayMode
      .endpointDiscoveryEnabled(false)
      .endpoint(cosmosDBEmulatorContainer.getEmulatorEndpoint)
      .key(cosmosDBEmulatorContainer.getEmulatorKey)
      .buildClient) { client =>

      client.createDatabase(DATABASE)
      val db = client.getDatabase(DATABASE)
      db.createContainer(CONTAINER, "/id")
      val container = db.getContainer(CONTAINER)
      for (i <- 1 to 10) {
        container.createItem(new Document("id", i.toString))
      }
    }
    if (triedCreateData.isFailure) {
      val throwable = triedCreateData.failed.get
      error("Error creando la data", throwable)
      throw throwable
    }
    info("Data creada ------------------------------------------------------------<")
  }

  @AfterClass
  def close(): Unit = {
    info("Stop CosmosDB emulator")
    cosmosDBEmulatorContainer.stop()
  }
}
