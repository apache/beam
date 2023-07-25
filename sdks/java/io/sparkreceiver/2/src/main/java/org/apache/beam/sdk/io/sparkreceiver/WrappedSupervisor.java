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
package org.apache.beam.sdk.io.sparkreceiver;

import java.nio.ByteBuffer;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StreamBlockId;
import org.apache.spark.streaming.receiver.BlockGenerator;
import org.apache.spark.streaming.receiver.BlockGeneratorListener;
import org.apache.spark.streaming.receiver.Receiver;
import org.apache.spark.streaming.receiver.ReceiverSupervisor;
import scala.Function0;
import scala.Option;
import scala.collection.Iterator;
import scala.collection.mutable.ArrayBuffer;

/** Wrapper class for {@link ReceiverSupervisor} that doesn't use Spark Environment. */
public class WrappedSupervisor extends ReceiverSupervisor {

  private final SparkConf sparkConf;
  private final SerializableFunction<Object[], Void> storeFn;

  public WrappedSupervisor(
      Receiver<?> receiver, SparkConf conf, SerializableFunction<Object[], Void> storeFn) {
    super(receiver, conf);
    this.sparkConf = conf;
    this.storeFn = storeFn;
  }

  @Override
  public void pushSingle(Object o) {
    storeFn.apply(new Object[] {o});
  }

  @Override
  public void pushBytes(
      ByteBuffer byteBuffer, Option<Object> option, Option<StreamBlockId> option1) {
    storeFn.apply(new Object[] {byteBuffer, option, option1});
  }

  @Override
  public void pushIterator(
      Iterator<?> iterator, Option<Object> option, Option<StreamBlockId> option1) {
    storeFn.apply(new Object[] {iterator, option, option1});
  }

  @Override
  public void pushArrayBuffer(
      ArrayBuffer<?> arrayBuffer, Option<Object> option, Option<StreamBlockId> option1) {
    storeFn.apply(new Object[] {arrayBuffer, option, option1});
  }

  @Override
  public BlockGenerator createBlockGenerator(BlockGeneratorListener blockGeneratorListener) {
    return new BlockGenerator(
        blockGeneratorListener,
        this.streamId(),
        this.sparkConf,
        BlockGenerator.$lessinit$greater$default$4());
  }

  @Override
  public void reportError(String s, Throwable throwable) {}

  @Override
  public boolean onReceiverStart() {
    return true;
  }

  @Override
  public long getCurrentRateLimit() {
    return Integer.MAX_VALUE;
  }

  @Override
  public boolean isReceiverStopped() {
    return super.isReceiverStopped();
  }

  @Override
  public void logInfo(Function0<String> msg) {
    // Do not log with Spark logging
  }
}
