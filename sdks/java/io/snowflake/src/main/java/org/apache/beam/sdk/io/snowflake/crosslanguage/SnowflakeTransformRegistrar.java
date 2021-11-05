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
package org.apache.beam.sdk.io.snowflake.crosslanguage;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/**
 * Exposes {@link SnowflakeIO.Read} and {@link SnowflakeIO.Write} as an external transform for
 * cross-language usage.
 */
@Experimental(Kind.PORTABILITY)
@AutoService(ExternalTransformRegistrar.class)
public final class SnowflakeTransformRegistrar implements ExternalTransformRegistrar {
  public static final String READ_URN = "beam:external:java:snowflake:read:v1";
  public static final String WRITE_URN = "beam:external:java:snowflake:write:v1";

  @Override
  public Map<String, ExternalTransformBuilder<?, ?, ?>> knownBuilderInstances() {
    return ImmutableMap.of(READ_URN, new ReadBuilder(), WRITE_URN, new WriteBuilder());
  }
}
