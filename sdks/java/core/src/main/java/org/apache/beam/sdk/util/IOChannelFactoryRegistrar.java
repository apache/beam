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
package org.apache.beam.sdk.util;

import com.google.auto.service.AutoService;
import java.util.ServiceLoader;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * A registrar that creates {@link IOChannelFactory} instances from {@link PipelineOptions}.
 *
 * <p>{@link IOChannelFactory} creators have the ability to provide a registrar by creating
 * a {@link ServiceLoader} entry and a concrete implementation of this interface.
 *
 * <p>It is optional but recommended to use one of the many build time tools such as
 * {@link AutoService} to generate the necessary META-INF files automatically.
 */
public interface IOChannelFactoryRegistrar {
  /**
   * Create a {@link IOChannelFactory} from the given {@link PipelineOptions}.
   */
  IOChannelFactory fromOptions(PipelineOptions options);

  /**
   * Get the URI scheme which defines the namespace of the IOChannelFactoryRegistrar.
   *
   * <p>The scheme is required to be unique among all
   * {@link IOChannelFactoryRegistrar IOChannelFactoryRegistrars}.
   *
   * @see <a href="https://www.ietf.org/rfc/rfc2396.txt">RFC 2396</a>
   */
  String getScheme();
}
