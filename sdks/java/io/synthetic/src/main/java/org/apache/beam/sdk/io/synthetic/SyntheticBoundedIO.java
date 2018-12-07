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
package org.apache.beam.sdk.io.synthetic;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * This {@link SyntheticBoundedIO} class provides a parameterizable batch custom source that is
 * deterministic.
 *
 * <p>To read a {@link PCollection} of {@code KV<byte[], byte[]>} from {@link SyntheticBoundedIO},
 * use {@link SyntheticBoundedIO#readFrom} to construct the synthetic source with synthetic source
 * options. See {@link SyntheticSourceOptions} for how to construct an instance. An example is
 * below:
 *
 * <pre>{@code
 * Pipeline p = ...;
 * SyntheticBoundedInput.SourceOptions sso = ...;
 *
 * // Construct the synthetic input with synthetic source options.
 * PCollection<KV<byte[], byte[]>> input = p.apply(SyntheticBoundedInput.readFrom(sso));
 * }</pre>
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class SyntheticBoundedIO {
  /** Read from the synthetic source options. */
  public static Read.Bounded<KV<byte[], byte[]>> readFrom(SyntheticSourceOptions options) {
    checkNotNull(options, "Input synthetic source options should not be null.");
    return Read.from(new SyntheticBoundedSource(options));
  }
}
