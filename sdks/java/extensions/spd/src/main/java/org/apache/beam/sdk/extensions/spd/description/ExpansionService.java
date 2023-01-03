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
package org.apache.beam.sdk.extensions.spd.description;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import edu.umd.cs.findbugs.annotations.Nullable;

public class ExpansionService {

  @Nullable
  @JsonSetter(nulls = Nulls.FAIL)
  String name;

  public String getName() { return name == null ? "" : name; }

  @Nullable
  @JsonSetter(nulls = Nulls.FAIL)
  String type; // local,container,remote
  public String geType() { return type == null ? "local" : type; }

  @Nullable String container;

  public String getContainer() { return container == null ? "" : container; }

  @Nullable String remoteAddress;

  public String getRemoteAddress() { return remoteAddress == null ? "" : remoteAddress; }
}
