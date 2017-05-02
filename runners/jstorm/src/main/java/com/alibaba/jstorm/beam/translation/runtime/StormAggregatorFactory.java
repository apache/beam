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
package com.alibaba.jstorm.beam.translation.runtime;

import java.util.Set;

import avro.shaded.com.google.common.collect.Sets;
import com.alibaba.jstorm.common.metric.AsmCounter;
import com.alibaba.jstorm.metric.MetricClient;
import org.apache.beam.runners.core.AggregatorFactory;
import org.apache.beam.runners.core.ExecutionContext;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Sum;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class StormAggregatorFactory implements AggregatorFactory {
    private MetricClient metricClient;

    public StormAggregatorFactory(MetricClient metricClient) {
        this.metricClient = checkNotNull(metricClient, "metricClient");
    }

    @Override
    public <InputT, AccumT, OutputT> Aggregator<InputT, OutputT> createAggregatorForDoFn(
            Class<?> fnClass,
            ExecutionContext.StepContext stepContext,
            String aggregatorName,
            CombineFn<InputT, AccumT, OutputT> combine) {
        AsmCounter counter = metricClient.registerCounter(aggregatorName);
        return new SumAggregator<>(counter, combine);
    }

    private static class SumAggregator<InputT, OutputT> implements Aggregator<InputT, OutputT> {

        private static final Set<Class<?>> SUPPORTED_SUM_COMBINE_FNS = Sets.<Class<?>>newHashSet(
                Sum.ofDoubles().getClass(),
                Sum.ofIntegers().getClass(),
                Sum.ofLongs().getClass());

        private final AsmCounter counter;
        private final CombineFn<InputT, ?, OutputT> combineFn;

        SumAggregator(AsmCounter counter, CombineFn<InputT, ?, OutputT> combineFn) {
            checkNotNull(combineFn, "combineFn");
            checkArgument(SUPPORTED_SUM_COMBINE_FNS.contains(combineFn.getClass()));
            this.counter = checkNotNull(counter, "counter");
            this.combineFn = combineFn;
        }

        @Override
        public void addValue(InputT inputT) {
            if (inputT instanceof Integer) {
                counter.update((Integer) inputT);
            } else if (inputT instanceof Long) {
                counter.update((Long) inputT);
            } else if (inputT instanceof Double) {
                counter.update((Double) inputT);
            } else {
                throw new RuntimeException("Not supported inputT type: " + inputT);
            }
        }

        @Override
        public String getName() {
            return counter.getMetricName();
        }

        @Override
        public CombineFn<InputT, ?, OutputT> getCombineFn() {
            return combineFn;
        }
    }
}
