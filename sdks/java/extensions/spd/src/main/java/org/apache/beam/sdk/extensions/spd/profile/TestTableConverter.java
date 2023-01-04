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
package org.apache.beam.sdk.extensions.spd.profile;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.auto.service.AutoService;
import org.apache.beam.sdk.extensions.spd.description.Source;
import org.apache.beam.sdk.extensions.spd.description.TableDesc;
import org.apache.beam.sdk.extensions.sql.meta.Table;

@AutoService(ProfileTableConverter.class)
public class TestTableConverter extends ProfileTableConverter {

  public TestTableConverter(JsonNode profile) {
    super(profile);
  }

  @Override
  public String convertsType() {
    return "test";
  }

  @Override
  public Table getSourceTable(String name, Source source, TableDesc desc) {
    return Table.builder().name(name).build();
  }

  @Override
  public Table getMaterializedTable(String name) {
    return Table.builder().name(name).build();
  }
}
