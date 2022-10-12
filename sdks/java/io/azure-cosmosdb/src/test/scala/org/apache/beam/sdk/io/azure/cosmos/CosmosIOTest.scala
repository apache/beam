package org.apache.beam.sdk.io.azure.cosmos

import com.azure.cosmos.CosmosClientBuilder
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.azure.cosmos.CosmosIOTest.{CONTAINER, DATABASE, cosmosDBEmulatorContainer}
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Count
import org.apache.beam.sdk.values.PCollection
import org.bson.Document
import org.junit._
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.LoggerFactory
import org.testcontainers.containers.CosmosDBEmulatorContainer
import org.testcontainers.utility.DockerImageName

import java.nio.file.Files
import scala.util.Using

@RunWith(classOf[JUnit4])
class CosmosIOTest {
  private val log = LoggerFactory.getLogger("CosmosIOTest")
  //  @(Rule @getter)
  //  val pipelineWrite: TestPipeline = TestPipeline.create
  //  @(Rule @getter)
  //  val pipelineRead: TestPipeline = TestPipeline.create

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
  private val log = LoggerFactory.getLogger("CosmosIOTest[Obj]")
  private val DOCKER_NAME = "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:latest"
  private val cosmosDBEmulatorContainer = new CosmosDBEmulatorContainer(DockerImageName.parse(DOCKER_NAME))
  private val DATABASE = "test"
  private val CONTAINER = "test"

  @BeforeClass
  def setup(): Unit = {
    log.info("Starting CosmosDB emulator")
    cosmosDBEmulatorContainer.start()

    val tempFolder = new TemporaryFolder
    tempFolder.create()
    val keyStoreFile = tempFolder.newFile("azure-cosmos-emulator.keystore").toPath
    val keyStore = cosmosDBEmulatorContainer.buildNewKeyStore
    keyStore.store(Files.newOutputStream(keyStoreFile.toFile.toPath), cosmosDBEmulatorContainer.getEmulatorKey.toCharArray)
    System.setProperty("javax.net.ssl.trustStore", keyStoreFile.toString)
    System.setProperty("javax.net.ssl.trustStorePassword", cosmosDBEmulatorContainer.getEmulatorKey)
    System.setProperty("javax.net.ssl.trustStoreType", "PKCS12")


    log.info("Creando la data -------------------------------------------------------->")
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
      log.error("Error creando la data", throwable)
      throw throwable
    }
    log.info("Data creada ------------------------------------------------------------<")
  }

  @AfterClass
  def close(): Unit = {
    log.info("Stop CosmosDB emulator")
    cosmosDBEmulatorContainer.stop()
  }
}
