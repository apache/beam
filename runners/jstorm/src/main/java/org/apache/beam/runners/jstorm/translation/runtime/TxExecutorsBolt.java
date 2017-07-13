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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.jstorm.cache.IKvStore;
import com.alibaba.jstorm.cache.IKvStoreManager;
import com.alibaba.jstorm.transactional.bolt.ITransactionStatefulBoltExecutor;
import java.io.IOException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TxExecutorsBolt implements ITransactionStatefulBoltExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(TxExecutorsBolt.class);

  private static final String TIME_SERVICE_STORE_ID = "timer_service_store";
  private static final String TIMER_SERVICE_KET = "timer_service_key";

  private ExecutorsBolt executorsBolt;
  private IKvStoreManager kvStoreManager;
  private IKvStore<String, TimerService> timerServiceStore;

  public TxExecutorsBolt(ExecutorsBolt executorsBolt) {
    this.executorsBolt = executorsBolt;
    this.executorsBolt.setStatefulBolt(true);
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    try {
      executorsBolt.prepare(stormConf, context, collector);
      kvStoreManager = executorsBolt.getExecutorContext().getKvStoreManager();
      timerServiceStore = kvStoreManager.getOrCreate(TIME_SERVICE_STORE_ID);
    } catch (IOException e) {
      LOG.error("Failed to prepare stateful bolt", e);
      throw new RuntimeException(e.getMessage());
    }
  }

  @Override
  public void execute(Tuple input) {
    executorsBolt.execute(input);
  }

  @Override
  public void cleanup() {
    executorsBolt.cleanup();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    executorsBolt.declareOutputFields(declarer);
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return executorsBolt.getComponentConfiguration();
  }

  @Override
  public void initState(Object userState) {
    LOG.info("Begin to init from state: {}", userState);
    restore(userState);
  }

  @Override
  public Object finishBatch(long batchId) {
    try {
      timerServiceStore.put(TIMER_SERVICE_KET, executorsBolt.timerService());
    } catch (IOException e) {
      LOG.error("Failed to store current timer service status", e);
      throw new RuntimeException(e.getMessage());
    }
    kvStoreManager.checkpoint(batchId);
    return null;
  }

  @Override
  public Object commit(long batchId, Object state) {
    return kvStoreManager.backup(batchId);
  }

  @Override
  public void rollBack(Object userState) {
    LOG.info("Begin to rollback from state: {}", userState);
    restore(userState);
  }

  @Override
  public void ackCommit(long batchId, long timeStamp) {
    kvStoreManager.remove(batchId);
  }

  private void restore(Object userState) {
    try {
      // restore all states
      kvStoreManager.restore(userState);

      // init timer service
      timerServiceStore = kvStoreManager.getOrCreate(TIME_SERVICE_STORE_ID);
      TimerService timerService = timerServiceStore.get(TIMER_SERVICE_KET);
      if (timerService == null) {
        timerService = executorsBolt.initTimerService();
      }
      executorsBolt.setTimerService(timerService);
    } catch (IOException e) {
      LOG.error("Failed to restore state", e);
      throw new RuntimeException(e.getMessage());
    }
  }
}