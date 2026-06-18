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
package org.apache.beam.runners.dataflow.worker.windmill.state;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.AbstractIterator;

/**
 * An iterable over elements backed by paginated GetData requests to Windmill. The iterable may be
 * iterated over an arbitrary number of times and multiple iterators may be active simultaneously.
 *
 * <p>There are two pattern we wish to support with low -memory and -latency:
 *
 * <ol>
 *   <li>Re-iterate over the initial elements multiple times (eg Iterables.first). We'll cache the
 *       initial 'page' of values returned by Windmill from our first request for the lifetime of
 *       the iterable.
 *   <li>Iterate through all elements of a very large collection. We'll send the GetData request for
 *       the next page when the current page is begun. We'll discard intermediate pages and only
 *       retain the first. Thus the maximum memory pressure is one page plus one page per call to
 *       iterator.
 * </ol>
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class PagingIterable<ContinuationT, ResultT> implements Iterable<ResultT> {
  /**
   * The reader we will use for scheduling continuation pages.
   *
   * <p>NOTE We've made this explicit to remind us to be careful not to cache the iterable.
   */
  private final WindmillStateReader reader;

  /** Initial values returned for the first page. Never reclaimed. */
  private final List<ResultT> firstPage;

  /** State tag with continuation position set for second page. */
  private final StateTag<ContinuationT> secondPagePos;

  /** Coder for elements. */
  private final Coder<?> coder;

  PagingIterable(
      WindmillStateReader reader,
      List<ResultT> firstPage,
      StateTag<ContinuationT> secondPagePos,
      Coder<?> coder) {
    this.reader = reader;
    this.firstPage = firstPage;
    this.secondPagePos = secondPagePos;
    this.coder = coder;
  }

  @Override
  public Iterator<ResultT> iterator() {
    return new PagingIterableIterator();
  }

  private class PagingIterableIterator extends AbstractIterator<ResultT> {
    private Iterator<ResultT> currentPage = firstPage.iterator();
    private StateTag<ContinuationT> nextPagePos = secondPagePos;
    private Future<ValuesAndContPosition<ResultT, ContinuationT>> pendingNextPage =
        // NOTE: The results of continuation page reads are never cached.
        reader.continuationFuture(nextPagePos, coder);

    @Override
    protected ResultT computeNext() {
      while (true) {
        if (currentPage.hasNext()) {
          return currentPage.next();
        }
        if (pendingNextPage == null) {
          return endOfData();
        }

        ValuesAndContPosition<ResultT, ContinuationT> valuesAndContPosition;
        try {
          valuesAndContPosition = pendingNextPage.get();
        } catch (InterruptedException | ExecutionException e) {
          if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
          }
          throw new RuntimeException("Unable to read value from state", e);
        }
        currentPage = valuesAndContPosition.getValues().iterator();
        StateTag.Builder<ContinuationT> nextPageBuilder =
            StateTag.of(
                    nextPagePos.getKind(),
                    nextPagePos.getTag(),
                    nextPagePos.getStateFamily(),
                    valuesAndContPosition.getContinuationPosition())
                .toBuilder();
        if (secondPagePos.getSortedListRange() != null) {
          nextPageBuilder.setSortedListRange(secondPagePos.getSortedListRange());
        }
        if (secondPagePos.getOmitValues() != null) {
          nextPageBuilder.setOmitValues(secondPagePos.getOmitValues());
        }
        if (secondPagePos.getMultimapKey() != null) {
          nextPageBuilder.setMultimapKey(secondPagePos.getMultimapKey());
        }
        nextPagePos = nextPageBuilder.build();
        pendingNextPage =
            // NOTE: The results of continuation page reads are never cached.
            reader.continuationFuture(nextPagePos, coder);
      }
    }
  }
}
