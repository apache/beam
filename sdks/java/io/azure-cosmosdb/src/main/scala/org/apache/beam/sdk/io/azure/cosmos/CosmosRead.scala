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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.beam.sdk.annotations.Experimental
import org.apache.beam.sdk.annotations.Experimental.Kind
import org.apache.beam.sdk.io.Read
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.{ PBegin, PCollection }
import org.bson.Document
import org.slf4j.LoggerFactory

/** A {@link PTransform} to read data from CosmosDB Core (SQL) API. */
@Experimental(Kind.SOURCE_SINK)
@SuppressFBWarnings(Array("MS_PKGPROTECT"))
case class CosmosRead(private[cosmos] val endpoint: String = null,
                      private[cosmos] val key: String = null,
                      private[cosmos] val database: String = null,
                      private[cosmos] val container: String = null,
                      private[cosmos] val query: String = null)
  extends PTransform[PBegin, PCollection[Document]] {


  private val log = LoggerFactory.getLogger(classOf[CosmosRead])

  /** Create new ReadCosmos based into previous ReadCosmos, modifying the endpoint */
  def withCosmosEndpoint(endpoint: String): CosmosRead = this.copy(endpoint = endpoint)

  def withCosmosKey(key: String): CosmosRead = this.copy(key = key)

  def withDatabase(database: String): CosmosRead = this.copy(database = database)

  def withQuery(query: String): CosmosRead = this.copy(query = query)

  def withContainer(container: String): CosmosRead = this.copy(container = container)

  override def expand(input: PBegin): PCollection[Document] = {
    log.debug(s"Read CosmosDB with endpoint: $endpoint and query: $query")
    validate()

    // input.getPipeline.apply(Read.from(new CosmosSource(this)))
    input.apply(Read.from(new CosmosBoundedSource(this)))
  }

  private def validate(): Unit = {
    require(endpoint != null, "CosmosDB endpoint is required")
    require(key != null, "CosmosDB key is required")
    require(database != null, "CosmosDB database is required")
    require(container != null, "CosmosDB container is required")
    require(query != null, "CosmosDB query is required")
  }
}
