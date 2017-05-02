/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.beam.translation.runtime;

import avro.shaded.com.google.common.collect.Maps;
import avro.shaded.com.google.common.collect.Sets;
import com.alibaba.jstorm.utils.Pair;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.TimeDomain;
import org.joda.time.Instant;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * Default implementation of {@link TimerService}.
 */
public class TimerServiceImpl implements TimerService {

    private final ConcurrentMap<Integer, Long> upStreamTaskToInputWatermark = new ConcurrentHashMap<>();
    private final PriorityQueue<Long> inputWatermarks = new PriorityQueue<>();
    private final PriorityQueue<Instant> watermarkHolds = new PriorityQueue<>();
    private final Map<String, Instant> namespaceToWatermarkHold = new HashMap<>();
    private transient final PriorityQueue<TimerInternals.TimerData> eventTimeTimersQueue = new PriorityQueue<>();
    private transient final Map<TimerInternals.TimerData, Set<Pair<DoFnExecutor, Object>>>
            timerDataToKeyedExecutors = Maps.newHashMap();

    private boolean initialized = false;

    public TimerServiceImpl() {
    }

    @Override
    public void init(List<Integer> upStreamTasks) {
        for (Integer task : upStreamTasks) {
            upStreamTaskToInputWatermark.put(task, BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis());
            inputWatermarks.add(BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis());
        }
        initialized = true;
    }

    @Override
    public synchronized void updateInputWatermark(Integer task, long taskInputWatermark) {
        checkState(initialized, "TimerService has not been initialized.");
        Long oldTaskInputWatermark = upStreamTaskToInputWatermark.get(task);
        // Make sure the input watermark don't go backward.
        if (taskInputWatermark > oldTaskInputWatermark) {
            upStreamTaskToInputWatermark.put(task, taskInputWatermark);
            inputWatermarks.add(taskInputWatermark);
            inputWatermarks.remove(oldTaskInputWatermark);

            long newLocalInputWatermark = currentInputWatermark();
            if (newLocalInputWatermark > oldTaskInputWatermark) {
                fireTimers(newLocalInputWatermark);
            }
        }
    }

    private void fireTimers(long newWatermark) {
        TimerInternals.TimerData timerData;
        while ((timerData = eventTimeTimersQueue.peek()) != null
                && timerData.getTimestamp().getMillis() <= newWatermark) {
            for (Pair<DoFnExecutor, Object> keyedExecutor : timerDataToKeyedExecutors.get(timerData)) {
                keyedExecutor.getFirst().onTimer(keyedExecutor.getSecond(), timerData);
            }
            eventTimeTimersQueue.remove();
            timerDataToKeyedExecutors.remove(timerData);
        }
    }

    @Override
    public long currentInputWatermark() {
        return initialized ? inputWatermarks.peek() : BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis();
    }

    @Override
    public long currentOutputWatermark() {
        if (watermarkHolds.isEmpty()) {
            return currentInputWatermark();
        } else {
            return Math.min(currentInputWatermark(), watermarkHolds.peek().getMillis());
        }
    }

    @Override
    public void clearWatermarkHold(String namespace) {
        Instant currentHold = namespaceToWatermarkHold.get(namespace);
        if (currentHold != null) {
            watermarkHolds.remove(currentHold);
            namespaceToWatermarkHold.remove(namespace);
        }
    }

    @Override
    public void addWatermarkHold(String namespace, Instant watermarkHold) {
        Instant currentHold = namespaceToWatermarkHold.get(namespace);
        if (currentHold == null) {
            namespaceToWatermarkHold.put(namespace, watermarkHold);
            watermarkHolds.add(watermarkHold);
        } else if (currentHold != null && watermarkHold.isBefore(currentHold)) {
            namespaceToWatermarkHold.put(namespace, watermarkHold);
            watermarkHolds.add(watermarkHold);
            watermarkHolds.remove(currentHold);
        }
    }

    @Override
    public void setTimer(Object key, TimerInternals.TimerData timerData, DoFnExecutor doFnExecutor) {
        checkArgument(
                TimeDomain.EVENT_TIME.equals(timerData.getDomain()),
                String.format("Does not support domain: %s.", timerData.getDomain()));
        Set<Pair<DoFnExecutor, Object>> keyedExecutors = timerDataToKeyedExecutors.get(timerData);
        if (keyedExecutors == null) {
            keyedExecutors = Sets.newHashSet();
            eventTimeTimersQueue.add(timerData);
        }
        keyedExecutors.add(new Pair<>(doFnExecutor, key));
        timerDataToKeyedExecutors.put(timerData, keyedExecutors);
    }
}