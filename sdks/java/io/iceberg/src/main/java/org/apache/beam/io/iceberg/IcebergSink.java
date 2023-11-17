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
package org.apache.beam.io.iceberg;

import com.google.common.collect.ImmutableList;
import java.util.UUID;
import org.apache.beam.io.iceberg.WriteBundlesToFiles.Result;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.ShardedKeyCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.ShardedKey;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.NotImplementedException;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.log4j.Logger;

public class IcebergSink<DestinationT extends Object, ElementT>
    extends PTransform<PCollection<KV<DestinationT, ElementT>>, IcebergWriteResult> {

  private static final Logger LOG = Logger.getLogger(IcebergSink.class);

  @VisibleForTesting static final int DEFAULT_MAX_WRITERS_PER_BUNDLE = 20;
  @VisibleForTesting static final int DEFAULT_MAX_FILES_PER_PARTITION = 10_000;
  @VisibleForTesting static final long DEFAULT_MAX_BYTES_PER_PARTITION = 10L * (1L << 40); // 10TB
  static final long DEFAULT_MAX_BYTES_PER_FILE = (1L << 40); // 1TB
  static final int DEFAULT_NUM_FILE_SHARDS = 0;
  static final int FILE_TRIGGERING_RECORD_COUNT = 50_000;

  final DynamicDestinations<?, DestinationT> dynamicDestinations;
  final Coder<DestinationT> destinationCoder;

  final RecordWriterFactory<ElementT, DestinationT> recordWriterFactory;
  final TableFactory<String> tableFactory;

  boolean triggered;

  public IcebergSink(
      DynamicDestinations<?, DestinationT> dynamicDestinations,
      Coder<DestinationT> destinationCoder,
      RecordWriterFactory<ElementT, DestinationT> recordWriterFactory,
      TableFactory<String> tableFactory) {
    this.dynamicDestinations = dynamicDestinations;
    this.destinationCoder = destinationCoder;
    this.triggered = false;
    this.recordWriterFactory = recordWriterFactory;
    this.tableFactory = tableFactory;
  }

  private IcebergWriteResult expandTriggered(PCollection<KV<DestinationT, ElementT>> input) {

    throw new NotImplementedException("Not yet implemented");
  }

  private IcebergWriteResult expandUntriggered(PCollection<KV<DestinationT, ElementT>> input) {

    final PCollectionView<String> fileView = createJobIdPrefixView(input.getPipeline());
    // We always do the equivalent of a dynamically sharded file creation
    TupleTag<WriteBundlesToFiles.Result<DestinationT>> writtenFilesTag =
        new TupleTag<>("writtenFiles");
    TupleTag<KV<ShardedKey<DestinationT>, ElementT>> successfulWritesTag =
        new TupleTag<>("successfulWrites");
    TupleTag<KV<ShardedKey<DestinationT>, ElementT>> failedWritesTag =
        new TupleTag<>("failedWrites");
    TupleTag<KV<TableIdentifier, Snapshot>> snapshotsTag = new TupleTag<>("snapshots");

    final Coder<ElementT> elementCoder =
        ((KvCoder<DestinationT, ElementT>) input.getCoder()).getValueCoder();

    // Write everything to files
    PCollectionTuple writeBundlesToFiles =
        input.apply(
            "Write Bundles To Files",
            ParDo.of(
                    new WriteBundlesToFiles<>(
                        fileView,
                        successfulWritesTag,
                        failedWritesTag,
                        DEFAULT_MAX_WRITERS_PER_BUNDLE,
                        DEFAULT_MAX_BYTES_PER_FILE,
                        recordWriterFactory))
                .withSideInputs(fileView)
                .withOutputTags(
                    writtenFilesTag,
                    TupleTagList.of(ImmutableList.of(successfulWritesTag, failedWritesTag))));

    PCollection<KV<ShardedKey<DestinationT>, ElementT>> successfulWrites =
        writeBundlesToFiles
            .get(successfulWritesTag)
            .setCoder(KvCoder.of(ShardedKeyCoder.of(destinationCoder), elementCoder));

    PCollection<KV<ShardedKey<DestinationT>, ElementT>> failedWrites =
        writeBundlesToFiles
            .get(failedWritesTag)
            .setCoder(KvCoder.of(ShardedKeyCoder.of(destinationCoder), elementCoder));

    PCollection<WriteBundlesToFiles.Result<DestinationT>> writtenFilesGrouped =
        failedWrites
            .apply("Group By Destination", GroupByKey.create())
            .apply(
                "Strip Shard ID",
                MapElements.via(
                    new SimpleFunction<
                        KV<ShardedKey<DestinationT>, Iterable<ElementT>>,
                        KV<DestinationT, Iterable<ElementT>>>() {
                      @Override
                      public KV<DestinationT, Iterable<ElementT>> apply(
                          KV<ShardedKey<DestinationT>, Iterable<ElementT>> input) {
                        return KV.of(input.getKey().getKey(), input.getValue());
                      }
                    }))
            .setCoder(KvCoder.of(destinationCoder, IterableCoder.of(elementCoder)))
            .apply(
                "Write Grouped Records",
                ParDo.of(
                    new WriteGroupedRecordsToFiles<>(
                        fileView, DEFAULT_MAX_BYTES_PER_FILE, recordWriterFactory)))
            .setCoder(WriteBundlesToFiles.ResultCoder.of(destinationCoder));

    PCollection<WriteBundlesToFiles.Result<DestinationT>> catalogUpdates =
        PCollectionList.of(
                writeBundlesToFiles
                    .get(writtenFilesTag)
                    .setCoder(WriteBundlesToFiles.ResultCoder.of(destinationCoder)))
            .and(writtenFilesGrouped)
            .apply("Flatten Files", Flatten.pCollections())
            .setCoder(WriteBundlesToFiles.ResultCoder.of(destinationCoder));

    // Apply any sharded writes and flatten everything for catalog updates
    PCollection<KV<String, Snapshot>> snapshots =
        catalogUpdates
            .apply(
                "Extract Data File",
                ParDo.of(
                    new DoFn<Result<DestinationT>, KV<String, MetadataUpdate>>() {
                      @ProcessElement
                      public void processElement(
                          ProcessContext c, @Element Result<DestinationT> element) {
                        c.output(
                            KV.of(
                                element.tableId,
                                MetadataUpdate.of(
                                    element.partitionSpec, element.update.getDataFiles().get(0))));
                      }
                    }))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), MetadataUpdate.coder()))
            .apply(GroupByKey.create())
            .apply("Write Metadata Updates", ParDo.of(new MetadataUpdates<>(tableFactory)))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Snapshot.class)));

    return new IcebergWriteResult(
        input.getPipeline(),
        successfulWrites,
        catalogUpdates,
        snapshots,
        successfulWritesTag,
        writtenFilesTag,
        snapshotsTag);
  }

  private PCollectionView<String> createJobIdPrefixView(Pipeline p) {

    final String jobName = p.getOptions().getJobName();

    return p.apply("JobIdCreationRoot_", Create.of((Void) null))
        .apply(
            "CreateJobId",
            ParDo.of(
                new DoFn<Void, String>() {
                  @ProcessElement
                  public void process(ProcessContext c) {
                    c.output(jobName + "-" + UUID.randomUUID().toString());
                  }
                }))
        .apply("JobIdSideInput", View.asSingleton());
  }

  public IcebergWriteResult expand(PCollection<KV<DestinationT, ElementT>> input) {

    String jobName = input.getPipeline().getOptions().getJobName();

    // We always window into global as far as I can tell?
    PCollection<KV<DestinationT, ElementT>> globalInput =
        input.apply(
            "rewindowIntoGlobal",
            Window.<KV<DestinationT, ElementT>>into(new GlobalWindows())
                .triggering(DefaultTrigger.of())
                .discardingFiredPanes());
    return triggered ? expandTriggered(input) : expandUntriggered(input);
  }
}
