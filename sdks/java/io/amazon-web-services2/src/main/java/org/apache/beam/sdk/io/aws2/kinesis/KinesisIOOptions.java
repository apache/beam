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
package org.apache.beam.sdk.io.aws2.kinesis;

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

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsRegistrar;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * PipelineOptions for {@link KinesisIO}.
 *
 * <p>Allows passing modify-able configurations in cases when some runner implementation persists
 * {@link KinesisIO.Read} serialized objects. Adding new configurations to this class should be
 * exceptional, and standard {@link KinesisIO.Read} / {@link KinesisIO.Write} should be preferred in
 * most of the cases.
 *
 * <p>This class appeared during the implementation of EFO consumer. In Flink runner, {@link
 * KinesisIO.Read} is serialized with the entire {@link KinesisSource} object which was a trouble
 * for EFO feature design: if consumer ARN is part of KinesisIO.Read object, when started from a
 * Flink savepoint, consumer ARN string or null value would be forced from the savepoint.
 *
 * <p>Consequences of this are:
 *
 * <ol>
 *   <li>Once a Kinesis source is started, its consumer ARN can't be changed without loosing state
 *       (checkpoint-ed shard progress).
 *   <li>Kinesis source can not have seamless enabling / disabling of EFO feature without loosing
 *       state (checkpoint-ed shard progress).
 * </ol>
 *
 * <p>This {@link PipelineOptions} extension allows having modifiable configurations for {@link
 * org.apache.beam.sdk.io.UnboundedSource#split(int, PipelineOptions)} and {@link
 * org.apache.beam.sdk.io.UnboundedSource#createReader(PipelineOptions,
 * UnboundedSource.CheckpointMark)}, which is essential for seamless EFO switch on / off.
 */
public interface KinesisIOOptions extends PipelineOptions {
  /**
   * Used to enable / disable EFO.
   *
   * <p>Example:
   *
   * <pre>{@code --kinesisIOConsumerArns={
   *   "stream-01": "arn:aws:kinesis:...:stream/stream-01/consumer/consumer-01:1678576714",
   *   "stream-02": "arn:aws:kinesis:...:stream/stream-02/consumer/my-consumer:1679576982",
   *   ...
   * }}</pre>
   */
  @Description("Mapping of streams' names to consumer ARNs of those streams.")
  @Default.InstanceFactory(MapFactory.class)
  Map<String, String> getKinesisIOConsumerArns();

  void setKinesisIOConsumerArns(Map<String, String> value);

  class MapFactory implements DefaultValueFactory<Map<String, String>> {

    @Override
    public Map<String, String> create(PipelineOptions options) {
      return ImmutableMap.of();
    }
  }

  /** A registrar containing the default {@link KinesisIOOptions}. */
  @AutoService(PipelineOptionsRegistrar.class)
  class KinesisIOOptionsRegistrar implements PipelineOptionsRegistrar {

    @Override
    public Iterable<Class<? extends PipelineOptions>> getPipelineOptions() {
      return ImmutableList.<Class<? extends PipelineOptions>>builder()
          .add(KinesisIOOptions.class)
          .build();
    }
  }
}
