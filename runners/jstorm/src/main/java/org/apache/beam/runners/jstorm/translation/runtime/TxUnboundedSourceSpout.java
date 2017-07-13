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
package org.apache.beam.runners.jstorm.translation.runtime;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import com.alibaba.jstorm.cache.IKvStore;
import com.alibaba.jstorm.cache.IKvStoreManager;
import com.alibaba.jstorm.cache.KvStoreManagerFactory;
import com.alibaba.jstorm.transactional.spout.ITransactionSpoutExecutor;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.sdk.io.UnboundedSource;
import org.slf4j.LoggerFactory;

public class TxUnboundedSourceSpout implements ITransactionSpoutExecutor {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TxUnboundedSourceSpout.class);

  private static final String SOURCE_STORE_ID = "SourceCheckpoint";
  private static final String CHECKPOINT_MARK = "CheckpointMark";

  private UnboundedSourceSpout sourceSpout;
  private UnboundedSource.UnboundedReader reader;
  private IKvStoreManager kvStoreManager;
  private IKvStore<String, UnboundedSource.CheckpointMark> sourceCheckpointStore;

  public TxUnboundedSourceSpout(UnboundedSourceSpout sourceSpout) {
    this.sourceSpout = sourceSpout;
  }

  private void restore(Object userState) {
    try {
      kvStoreManager.restore(userState);
      sourceCheckpointStore = kvStoreManager.getOrCreate(SOURCE_STORE_ID);
      UnboundedSource.CheckpointMark checkpointMark = sourceCheckpointStore.get(CHECKPOINT_MARK);
      sourceSpout.createSourceReader(checkpointMark);
      reader = sourceSpout.getUnboundedSourceReader();
    } catch (IOException e) {
      LOG.error("Failed to init state", e);
      throw new RuntimeException(e.getMessage());
    }
  }

  @Override
  public void initState(Object userState) {
    restore(userState);
  }

  @Override
  public Object finishBatch(long checkpointId) {
    try {
      // Store check point mark from unbounded source reader
      UnboundedSource.CheckpointMark checkpointMark = reader.getCheckpointMark();
      sourceCheckpointStore.put(CHECKPOINT_MARK, checkpointMark);

      // checkpoint all kv stores in current manager
      kvStoreManager.checkpoint(checkpointId);
    } catch (IOException e) {
      LOG.error(String.format("Failed to finish batch-%s", checkpointId), e);
      throw new RuntimeException(e.getMessage());
    }
    return null;
  }

  @Override
  public Object commit(long batchId, Object state) {
    // backup kv stores to remote state backend
    return kvStoreManager.backup(batchId);
  }

  @Override
  public void rollBack(Object userState) {
    restore(userState);
  }

  @Override
  public void ackCommit(long batchId, long timeStamp) {
    // remove obsolete state in bolt local and remote state backend
    kvStoreManager.remove(batchId);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    sourceSpout.declareOutputFields(declarer);
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return sourceSpout.getComponentConfiguration();
  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    try {
      sourceSpout.open(conf, context, collector);
      String storeName = String.format("task-%s", context.getThisTaskId());
      String storePath = String.format("%s/beam/%s", context.getWorkerIdDir(), storeName);
      kvStoreManager = KvStoreManagerFactory.getKvStoreManagerWithMonitor(context, storeName, storePath, true);

      reader = sourceSpout.getUnboundedSourceReader();
    } catch (IOException e) {
      LOG.error("Failed to open transactional unbounded source spout", e);
      throw new RuntimeException(e.getMessage());
    }
  }

  @Override
  public void close() {
    sourceSpout.close();
  }

  @Override
  public void activate() {
    sourceSpout.activate();
  }

  @Override
  public void deactivate() {
    sourceSpout.deactivate();
  }

  @Override
  public void nextTuple() {
    sourceSpout.nextTuple();
  }

  @Override
  public void ack(Object msgId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fail(Object msgId) {
    throw new UnsupportedOperationException();
  }
}