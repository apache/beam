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
package org.apache.beam.sdk.extensions.euphoria.core.executor.io;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.ExternalIterable;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.SpillTools;
import org.apache.beam.sdk.extensions.euphoria.core.executor.Constants;
import org.apache.beam.sdk.extensions.euphoria.core.util.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An implementation of {@code SpillTools} to be used by executors. */
@Audience(Audience.Type.EXECUTOR)
public class GenericSpillTools implements SpillTools {

  private static final Logger LOG = LoggerFactory.getLogger(GenericSpillTools.class);

  /** Number of records to keep in list before spilling. */
  private final int numSpillRecords;

  /** Factory for creating files for spilling. */
  private final SpillFileFactory spillFactory;

  /** Factory for serializer used when spilling data to disk. */
  private final SerializerFactory serializer;

  /**
   * @param serializer path to temporary directory to use for spilling
   * @param spillFactory factory for creating files for spilling
   * @param settings settings to read configuration from
   */
  public GenericSpillTools(
      SerializerFactory serializer, SpillFileFactory spillFactory, Settings settings) {

    this(
        serializer,
        spillFactory,
        settings.getInt(Constants.SPILL_BUFFER_ITEMS, Constants.SPILL_BUFFER_ITEMS_DEFAULT));
  }

  /**
   * @param serializer path to temporary directory to use for spilling
   * @param settings settings to read configuration from
   */
  public GenericSpillTools(SerializerFactory serializer, Settings settings) {

    this(
        serializer,
        spillFactory(settings),
        settings.getInt(Constants.SPILL_BUFFER_ITEMS, Constants.SPILL_BUFFER_ITEMS_DEFAULT));
  }

  @VisibleForTesting
  GenericSpillTools(SerializerFactory serializer, SpillFileFactory spillFactory, int spillRecords) {

    this.serializer = serializer;
    this.spillFactory = spillFactory;
    this.numSpillRecords = spillRecords;
  }

  private static SpillFileFactory spillFactory(Settings settings) {
    final File tmpDir =
        new File(settings.getString(Constants.LOCAL_TMP_DIR, Constants.LOCAL_TMP_DIR_DEFAULT));

    if (tmpDir.exists()) {
      if (!tmpDir.isDirectory()) {
        throw new IllegalArgumentException(
            "Path "
                + tmpDir
                + " exists and is not directory! Tune your "
                + Constants.LOCAL_TMP_DIR
                + " settings");
      }
    } else {
      if (!tmpDir.mkdirs()) {
        throw new IllegalStateException("Unable to create '" + tmpDir + "' directory.");
      }
    }
    return () ->
        new File(tmpDir, String.format("euphoria-spill-%s.bin", UUID.randomUUID().toString()));
  }

  @Override
  public <T> ExternalIterable<T> externalize(Iterable<T> what) {
    return externalize(StreamSupport.stream(what.spliterator(), false));
  }

  private <T> ExternalIterable<T> externalize(Stream<T> what) {
    FsSpillingListStorage<T> ret =
        new FsSpillingListStorage<>(serializer, spillFactory, numSpillRecords);
    what.forEach(ret::add);
    ret.closeOutput();
    return ret;
  }

  @Override
  public <T> Collection<ExternalIterable<T>> spillAndSortParts(
      Iterable<T> what, Comparator<T> comparator) throws InterruptedException {

    List<ExternalIterable<T>> ret = new ArrayList<>();
    List<T> sortList = new ArrayList<>(numSpillRecords);
    for (T e : what) {
      if (sortList.size() == numSpillRecords) {
        ret.add(externalize(sortList.stream().sorted(comparator)));
        LOG.debug("Successfully externalized {} records", sortList.size());
        sortList.clear();
      }
      sortList.add(e);
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException();
      }
    }
    if (!sortList.isEmpty()) {
      ret.add(externalize(sortList.stream().sorted(comparator)));
    }

    return ret;
  }
}
