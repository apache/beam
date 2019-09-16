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
package org.apache.beam.runners.apex.translation.utils;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.netlet.util.Slice;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;

/** The Apex {@link StreamCodec} adapter for using Beam {@link Coder}. */
public class CoderAdapterStreamCodec implements StreamCodec<Object>, Serializable {
  private static final long serialVersionUID = 1L;
  private final Coder<? super Object> coder;

  public CoderAdapterStreamCodec(Coder<? super Object> coder) {
    this.coder = coder;
  }

  @VisibleForTesting
  public Coder<?> getCoder() {
    return this.coder;
  }

  @Override
  public Object fromByteArray(Slice fragment) {
    ByteArrayInputStream bis =
        new ByteArrayInputStream(fragment.buffer, fragment.offset, fragment.length);
    try {
      return coder.decode(bis, Context.OUTER);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Slice toByteArray(Object wv) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      coder.encode(wv, bos, Context.OUTER);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return new Slice(bos.toByteArray());
  }

  @Override
  public int getPartition(Object o) {
    return o.hashCode();
  }
}
