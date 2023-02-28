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
package org.apache.beam.sdk.io.azure.cosmos;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.azure.cosmos.*;
import com.azure.cosmos.implementation.*;
import com.azure.cosmos.implementation.feedranges.FeedRangeInternal;
import com.azure.cosmos.models.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
import reactor.core.publisher.Mono;

@Experimental(Experimental.Kind.SOURCE_SINK)
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class CosmosIO {

  private CosmosIO() {}

  /** A POJO describing a connection configuration to Cosmos DB. */
  @AutoValue
  public abstract static class ConnectionConfiguration implements Serializable {

    public abstract String getEndpoint();

    public abstract @Nullable String getKey();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setEndpoint(String endpoint);

      abstract Builder setKey(String key);

      abstract ConnectionConfiguration build();
    }

    public static ConnectionConfiguration create(String endpoint) {
      return new AutoValue_CosmosIO_ConnectionConfiguration.Builder().setEndpoint(endpoint).build();
    }

    /** */
    public ConnectionConfiguration withEndpoint(String endpoint) {
      checkArgument(endpoint != null, "endpoint can not be null");
      checkArgument(!endpoint.isEmpty(), "endpoint can not be empty");
      return builder().setEndpoint(endpoint).build();
    }

    /** */
    public ConnectionConfiguration withKey(String key) {
      checkArgument(key != null, "key can not be null");
      checkArgument(!key.isEmpty(), "key can not be empty");
      return builder().setKey(key).build();
    }

    CosmosAsyncClient createClient() throws IOException {
      return new CosmosClientBuilder().endpoint(getEndpoint()).key(getKey()).buildAsyncClient();
    }
  }

  /** Provide a {@link Read} {@link PTransform} to read data from a Cosmos DB. */
  public static <T> Read<T> read(Class<T> classType) {
    return new AutoValue_CosmosIO_Read.Builder<T>().setClassType(classType).build();
  }

  @AutoValue
  @AutoValue.CopyAnnotations
  @SuppressWarnings({"rawtypes"})
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {

    abstract @Nullable Class<T> getClassType();

    abstract @Nullable ConnectionConfiguration getConnectionConfiguration();

    abstract @Nullable String getDatabase();

    abstract @Nullable String getContainer();

    abstract @Nullable Coder<T> getCoder();

    abstract Builder<T> builder();

    @AutoValue.Builder
    abstract static class Builder<T> {

      abstract Builder<T> setClassType(Class<T> classType);

      abstract Builder<T> setConnectionConfiguration(
          ConnectionConfiguration connectionConfiguration);

      abstract Builder<T> setDatabase(String database);

      abstract Builder<T> setContainer(String container);

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract Read<T> build();
    }

    /** */
    public Read<T> withConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
      checkArgument(connectionConfiguration != null, "connectionConfiguration can not be null");
      return builder().setConnectionConfiguration(connectionConfiguration).build();
    }

    /** */
    public Read<T> withDatabase(String database) {
      checkArgument(database != null, "database can not be null");
      checkArgument(!database.isEmpty(), "database can not be empty");
      return builder().setDatabase(database).build();
    }

    /** */
    public Read<T> withContainer(String container) {
      checkArgument(container != null, "container can not be null");
      checkArgument(!container.isEmpty(), "container can not be empty");
      return builder().setContainer(container).build();
    }

    public Read<T> withCoder(Coder<T> coder) {
      checkArgument(coder != null, "coder can not be null");
      return builder().setCoder(coder).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      checkState(getConnectionConfiguration() != null, "withConnectionConfiguration() is required");
      checkState(getDatabase() != null, "withDatabase() is required");
      checkState(getContainer() != null, "withContainer() is required");
      checkState(getCoder() != null, "withContainer() is required");
      return input.apply(org.apache.beam.sdk.io.Read.from(new BoundedCosmosBDSource<>(this)));
    }
  }

  /** A {@link BoundedSource} reading from Comos. */
  @VisibleForTesting
  public static class BoundedCosmosBDSource<T> extends BoundedSource<T> {

    private final Read<T> spec;
    private final NormalizedRange range;

    private @Nullable Long estimatedByteSize;

    BoundedCosmosBDSource(Read<T> spec) {
      this(spec, NormalizedRange.FULL_RANGE, null);
    }

    BoundedCosmosBDSource(Read<T> spec, NormalizedRange range, Long estimatedSize) {
      this.spec = spec;
      this.range = range;
      this.estimatedByteSize = estimatedSize;
    }

    @Override
    public List<? extends BoundedSource<T>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      try (CosmosAsyncClient client = spec.getConnectionConfiguration().createClient()) {
        CosmosAsyncDatabase database = client.getDatabase(spec.getDatabase());
        CosmosAsyncContainer container = database.getContainer(spec.getContainer());
        AsyncDocumentClient document = CosmosBridgeInternal.getAsyncDocumentClient(client);

        List<BoundedCosmosBDSource<T>> sources = new ArrayList<>();
        long rangeSize = getEstimatedSizeBytes(options);
        float splitsFloat = (float) rangeSize / desiredBundleSizeBytes;
        int splits = (int) Math.ceil(splitsFloat);

        // match internal impl of CosmosAsyncContainer trySplitFeedRange
        String databaseLink =
            ImplementationBridgeHelpers.CosmosAsyncDatabaseHelper.getCosmosAsyncDatabaseAccessor()
                .getLink(database);
        String containerLink =
            databaseLink + "/" + Paths.COLLECTIONS_PATH_SEGMENT + "/" + container.getId();
        Mono<Utils.ValueHolder<DocumentCollection>> getCollectionObservable =
            document
                .getCollectionCache()
                .resolveByNameAsync(null, containerLink, null)
                .map(Utils.ValueHolder::initialize);

        List<NormalizedRange> subRanges =
            FeedRangeInternal.convert(range.toFeedRange())
                .trySplit(
                    document.getPartitionKeyRangeCache(), null, getCollectionObservable, splits)
                .block().stream()
                .map(NormalizedRange::fromFeedRange)
                .collect(Collectors.toList());

        long estimatedSubRangeSize = rangeSize / subRanges.size();
        for (NormalizedRange subrange : subRanges) {
          sources.add(new BoundedCosmosBDSource<>(spec, subrange, estimatedSubRangeSize));
        }

        return sources;
      }
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      if (estimatedByteSize != null) {
        return estimatedByteSize;
      }
      try (CosmosAsyncClient client = spec.getConnectionConfiguration().createClient()) {
        CosmosAsyncContainer container =
            client.getDatabase(spec.getDatabase()).getContainer(spec.getContainer());

        CosmosChangeFeedRequestOptions requestOptions =
            CosmosChangeFeedRequestOptions.createForProcessingFromNow(range.toFeedRange());
        requestOptions.setMaxItemCount(1);
        requestOptions.setMaxPrefetchPageCount(1);
        requestOptions.setQuotaInfoEnabled(true);

        estimatedByteSize =
            container
                .queryChangeFeed(requestOptions, ObjectNode.class)
                .byPage()
                .take(1)
                .map(FeedResponse::getDocumentUsage)
                .map(kb -> kb * 1000)
                .single()
                .block();

        return estimatedByteSize == null ? 0 : estimatedByteSize;
      }
    }

    @Override
    public Coder<T> getOutputCoder() {
      return spec.getCoder();
    }

    @Override
    public BoundedReader<T> createReader(PipelineOptions options) throws IOException {
      return new BoundedCosmosReader<>(this);
    }
  }

  private static class BoundedCosmosReader<T> extends BoundedSource.BoundedReader<T> {

    private final BoundedCosmosBDSource<T> source;

    private CosmosAsyncClient client;

    private T current;
    private Iterator<T> iterator;

    private BoundedCosmosReader(BoundedCosmosBDSource<T> source) {
      this.source = source;
    }

    @Override
    public boolean start() throws IOException {
      client = source.spec.getConnectionConfiguration().createClient();
      String database = source.spec.getDatabase();
      String container = source.spec.getContainer();
      Class<T> classType = source.spec.getClassType();
      CosmosAsyncContainer c = client.getDatabase(database).getContainer(container);

      // custom options with query plan disabled
      CosmosQueryRequestOptions queryOptions =
          ImplementationBridgeHelpers.CosmosQueryRequestOptionsHelper
              .getCosmosQueryRequestOptionsAccessor()
              .disallowQueryPlanRetrieval(new CosmosQueryRequestOptions())
              .setFeedRange(source.range.toFeedRange());

      // TODO
      SqlQuerySpec query = new SqlQuerySpec("SELECT * FROM r");
      iterator = c.queryItems(query, queryOptions, classType).toIterable().iterator();

      return readNext();
    }

    @Override
    public boolean advance() throws IOException {
      return readNext();
    }

    private boolean readNext() {
      boolean nonEmpty = iterator.hasNext();
      if (nonEmpty) {
        current = iterator.next();
      }
      return nonEmpty;
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return current;
    }

    @Override
    public void close() throws IOException {
      if (client != null) {
        client.close();
      }
    }

    @Override
    public BoundedSource<T> getCurrentSource() {
      return source;
    }
  }
}
