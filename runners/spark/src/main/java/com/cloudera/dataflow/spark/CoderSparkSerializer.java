/**
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
package com.cloudera.dataflow.spark;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import org.apache.spark.serializer.DeserializationStream;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import scala.reflect.ClassTag;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

public class CoderSparkSerializer extends Serializer implements Serializable {

  @Override
  public SerializerInstance newInstance() {
    return new CoderSerializerInstance();
  }

  private class CoderSerializerInstance extends SerializerInstance implements Serializable {

    @Override
    public <T> ByteBuffer serialize(T t, ClassTag<T> ev) {
      return null;
    }

    @Override
    public <T> T deserialize(ByteBuffer bytes, ClassTag<T> ev) {
      return null;
    }

    @Override
    public <T> T deserialize(ByteBuffer bytes, ClassLoader loader, ClassTag<T> ev) {
      return null;
    }

    @Override
    public SerializationStream serializeStream(OutputStream os) {
      return null;
    }

    @Override
    public DeserializationStream deserializeStream(InputStream is) {
      return null;
    }
  }
}
