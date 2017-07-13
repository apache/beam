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

import static com.google.common.base.Preconditions.checkNotNull;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.utils.KryoSerializer;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.runners.jstorm.JStormPipelineOptions;
import org.apache.beam.runners.jstorm.translation.util.CommonInstance;
import org.apache.beam.runners.jstorm.util.SerializedPipelineOptions;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spout implementation that wraps a Beam UnboundedSource
 * <p>
 * TODO: add wrapper to support metrics in UnboundedSource.
 */
public class UnboundedSourceSpout extends AdaptorBasicSpout {
  private static final Logger LOG = LoggerFactory.getLogger(UnboundedSourceSpout.class);

  private final String description;
  private final UnboundedSource source;
  private final SerializedPipelineOptions serializedOptions;
  private final TupleTag<?> outputTag;

  private transient JStormPipelineOptions pipelineOptions;
  private transient UnboundedSource.UnboundedReader reader;
  private transient SpoutOutputCollector collector;

  private volatile boolean hasNextRecord;
  private AtomicBoolean activated = new AtomicBoolean();

  private KryoSerializer<WindowedValue> serializer;

  private long lastWaterMark = 0l;

  public UnboundedSourceSpout(
      String description,
      UnboundedSource source,
      JStormPipelineOptions options,
      TupleTag<?> outputTag) {
    this.description = checkNotNull(description, "description");
    this.source = checkNotNull(source, "source");
    this.serializedOptions = new SerializedPipelineOptions(checkNotNull(options, "options"));
    this.outputTag = checkNotNull(outputTag, "outputTag");
  }

  @Override
  public synchronized void close() {
    try {
      activated.set(false);
      this.reader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void activate() {
    activated.set(true);

  }

  @Override
  public void deactivate() {
    activated.set(false);
  }

  @Override
  public void ack(Object msgId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fail(Object msgId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    try {
      this.collector = collector;
      this.pipelineOptions =
          this.serializedOptions.getPipelineOptions().as(JStormPipelineOptions.class);

      createSourceReader(null);

      this.serializer = new KryoSerializer<>(conf);
    } catch (IOException e) {
      throw new RuntimeException("Unable to create unbounded reader.", e);
    }
  }

  public void createSourceReader(UnboundedSource.CheckpointMark checkpointMark) throws IOException {
    if (reader != null) {
      reader.close();
    }
    reader = this.source.createReader(this.pipelineOptions, checkpointMark);
    hasNextRecord = this.reader.start();
  }

  @Override
  public synchronized void nextTuple() {
    if (!activated.get()) {
      return;
    }
    try {
      if (!hasNextRecord) {
        hasNextRecord = reader.advance();
      }

      while (hasNextRecord && activated.get()) {
        Object value = reader.getCurrent();
        Instant timestamp = reader.getCurrentTimestamp();

        WindowedValue wv =
            WindowedValue.of(value, timestamp, GlobalWindow.INSTANCE, PaneInfo.NO_FIRING);
        LOG.debug("Source output: " + wv.getValue());
        if (keyedEmit(outputTag.getId())) {
          KV kv = (KV) wv.getValue();
          // Convert WindowedValue<KV> to <K, WindowedValue<V>>
          byte[] immutableValue = serializer.serialize(wv.withValue(kv.getValue()));
          collector.emit(outputTag.getId(), new Values(kv.getKey(), immutableValue));
        } else {
          byte[] immutableValue = serializer.serialize(wv);
          collector.emit(outputTag.getId(), new Values(immutableValue));
        }

        // move to next record
        hasNextRecord = reader.advance();
      }

      Instant waterMark = reader.getWatermark();
      if (waterMark != null && lastWaterMark < waterMark.getMillis()) {
        lastWaterMark = waterMark.getMillis();
        collector.flush();
        collector.emit(CommonInstance.BEAM_WATERMARK_STREAM_ID, new Values(waterMark.getMillis()));
        LOG.debug("Source output: WM-{}", waterMark.toDateTime());
      }
    } catch (IOException e) {
      throw new RuntimeException("Exception reading values from source.", e);
    }
  }

  public UnboundedSource getUnboundedSource() {
    return source;
  }

  public UnboundedSource.UnboundedReader getUnboundedSourceReader() {
    return reader;
  }

  @Override
  public String toString() {
    return description;
  }
}
