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
package org.apache.beam.sdk.transforms;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.values.PCollection;

public class BatchElements<T> extends PTransform<PCollection<T>, PCollection<List<T>>> {

    public class BatchSizeEstimator {
    private List<long[]> data = new ArrayList<>();

    public class Stopwatch implements AutoCloseable {
        private final long startTime;
        private final int batchSize;

        public Stopwatch(int batchSize) {
            this.batchSize = batchSize;
            this.startTime = System.currentTimeMillis();
        }

        @Override
        public void close() {
            long elapsed = System.currentTimeMillis() - startTime;
            data.add(new long[]{batchSize, elapsed}); 
        }
    }

    public Stopwatch recordTime(int batchSize) {
        return new Stopwatch(batchSize); 
    }

    public int nextBatchSize() {
        
    }
}
   

    @Override
    public PCollection<List<T>> expand(PCollection<T> input) {
        
    }
}