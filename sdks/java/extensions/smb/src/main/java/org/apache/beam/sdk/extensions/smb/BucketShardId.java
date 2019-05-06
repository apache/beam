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
package org.apache.beam.sdk.extensions.smb;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.VarIntCoder;

@AutoValue
public abstract class BucketShardId {

  public abstract int getBucketId();

  public abstract int getShardId();

  public static BucketShardId of(int bucketId, int shardId) {
    return new AutoValue_BucketShardId(bucketId, shardId);
  }

  public static class BucketShardIdCoder extends AtomicCoder<BucketShardId> {

    public static BucketShardIdCoder of() {
      return INSTANCE;
    }

    private static final BucketShardIdCoder INSTANCE = new BucketShardIdCoder();
    private static final VarIntCoder intCoder = VarIntCoder.of();

    private BucketShardIdCoder() {}

    @Override
    public void encode(BucketShardId value, OutputStream outStream)
        throws CoderException, IOException {
      intCoder.encode(value.getBucketId(), outStream);
      intCoder.encode(value.getShardId(), outStream);
    }

    @Override
    public BucketShardId decode(InputStream inStream) throws CoderException, IOException {
      return BucketShardId.of(intCoder.decode(inStream), intCoder.decode(inStream));
    }
  }
}
