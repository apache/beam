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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.cache;

import com.google.cloud.Timestamp;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Synchronously compute the earliest partition watermark, by delegating the call to {@link
 * PartitionMetadataDao#getUnfinishedMinWatermark()}.
 */
public class NoOpWatermarkCache implements WatermarkCache {

  private final PartitionMetadataDao dao;

  private static final Logger LOG = LoggerFactory.getLogger(NoOpWatermarkCache.class);

  public NoOpWatermarkCache(PartitionMetadataDao dao) {
    LOG.info("changliiu NoOpWatermarkCache");
    this.dao = dao;
  }

  @Override
  public @Nullable Timestamp getUnfinishedMinWatermark() {
    return dao.getUnfinishedMinWatermark(Optional.empty());
    // return dao.getUnfinishedMinWatermark();
    // changliiu remove
  }
}
