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

import com.azure.cosmos.models.CosmosQueryRequestOptions
import com.azure.cosmos.{ CosmosClient, CosmosClientBuilder }
import org.apache.beam.sdk.annotations.Experimental
import org.apache.beam.sdk.annotations.Experimental.Kind
import org.apache.beam.sdk.io.BoundedSource
import org.bson.Document
import org.slf4j.LoggerFactory


@Experimental(Kind.SOURCE_SINK)
private  class CosmosBoundedReader(cosmosSource: CosmosBoundedSource) extends BoundedSource.BoundedReader[Document] {
  private val log = LoggerFactory.getLogger(getClass)
  private var maybeClient: Option[CosmosClient] = None
  private var maybeIterator: Option[java.util.Iterator[Document]] = None

  override def start(): Boolean = {
    maybeClient = Some(
      new CosmosClientBuilder()
        .gatewayMode
        .endpointDiscoveryEnabled(false)
        .endpoint(cosmosSource.readCosmos.endpoint)
        .key(cosmosSource.readCosmos.key)
        .buildClient
    )

    maybeIterator = maybeClient.map { client =>
      log.info("Get the container name")

      log.info(s"Get the iterator of the query in container ${cosmosSource.readCosmos.container}")
      client
        .getDatabase(cosmosSource.readCosmos.database)
        .getContainer(cosmosSource.readCosmos.container)
        .queryItems(cosmosSource.readCosmos.query, new CosmosQueryRequestOptions(), classOf[Document])
        .iterator()
    }

    true
  }

  override def advance(): Boolean = maybeIterator.exists(_.hasNext)

  override def getCurrent: Document = maybeIterator
    .filter(_.hasNext)
    //.map(iterator => new Document(iterator.next()))
    .map(_.next())
    .orNull

  override def getCurrentSource: CosmosBoundedSource = cosmosSource

  override def close(): Unit = maybeClient.foreach(_.close())
}
