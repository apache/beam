/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.dataflow.spark;

/**
 * A marker interface that implementations of
 * {@link org.apache.hadoop.mapreduce.lib.output.FileOutputFormat} implement to indicate
 * that they produce shard names that adhere to the template in
 * {@link com.cloudera.dataflow.hadoop.HadoopIO.Write}.
 *
 * Some common shard names are defined in
 * {@link com.google.cloud.dataflow.sdk.io.ShardNameTemplate}.
 */
public interface ShardNameTemplateAware {
}
