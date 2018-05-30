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
package org.apache.beam.sdk.extensions.sql;

import org.apache.beam.sdk.extensions.sql.meta.provider.pubsub.PubsubJsonTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.junit.Ignore;
import org.junit.Test;

/** Unit tests for {@link BeamSqlCli} Pubsub functionality. */
public class BeamSqlCliPubsubTest {

  @Test
  @Ignore("Something like this needs an emulator. TODO: BEAM-4195")
  public void testPubsubTable() throws Exception {
    String pubsubTopic = "projects/<probject>/topics/<topic>";
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new PubsubJsonTableProvider());

    BeamSqlCli cli = new BeamSqlCli().metaStore(metaStore);

    cli.execute(
        "CREATE TABLE topic (\n"
            + "event_timestamp TIMESTAMP, \n"
            + "attributes MAP<VARCHAR, VARCHAR>, \n"
            + "payload ROW< \n"
            + "             `id` INTEGER, \n"
            + "             `name` VARCHAR \n"
            + "           > \n"
            + ") \n"
            + "TYPE 'pubsub' \n"
            + "LOCATION '"
            + pubsubTopic
            + "' \n"
            + "TBLPROPERTIES '{ \"timestampAttributeKey\" : \"ts\" }'");

    cli.execute("SELECT topic.payload.name from topic");
  }
}
