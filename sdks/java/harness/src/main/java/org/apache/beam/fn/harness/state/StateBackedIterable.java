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
package org.apache.beam.fn.harness.state;

import static org.apache.beam.runners.core.construction.ModelCoders.STATE_BACKED_ITERABLE_CODER_URN;

import com.google.auto.service.AutoService;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectStreamException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.Cache;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.runners.core.construction.CoderTranslation.TranslationContext;
import org.apache.beam.runners.core.construction.CoderTranslator;
import org.apache.beam.runners.core.construction.CoderTranslatorRegistrar;
import org.apache.beam.sdk.coders.IterableLikeCoder;
import org.apache.beam.sdk.fn.stream.PrefetchableIterable;
import org.apache.beam.sdk.fn.stream.PrefetchableIterators;
import org.apache.beam.sdk.util.BufferedElementCountingOutputStream;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterable;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterator;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BeamFnStateClient state} backed iterable which allows for fetching elements over the
 * portability state API. See <a
 * href="https://s.apache.org/beam-fn-state-api-and-bundle-processing">remote references</a> for
 * additional details.
 *
 * <p>One must supply a {@link StateBackedIterableTranslationContext} when using {@link
 * CoderTranslator#fromComponents} to be able to create a {@link StateBackedIterable.Coder}.
 */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class StateBackedIterable<T>
    extends ElementByteSizeObservableIterable<T, ElementByteSizeObservableIterator<T>>
    implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(StateBackedIterable.class);

  @VisibleForTesting final StateRequest request;
  @VisibleForTesting final List<T> prefix;
  private final transient PrefetchableIterable<T> suffix;

  private final org.apache.beam.sdk.coders.Coder<T> elemCoder;

  public StateBackedIterable(
      Cache<?, ?> cache,
      BeamFnStateClient beamFnStateClient,
      String instructionId,
      StateKey stateKey,
      org.apache.beam.sdk.coders.Coder<T> elemCoder,
      List<T> prefix) {
    this.request =
        StateRequest.newBuilder().setInstructionId(instructionId).setStateKey(stateKey).build();
    this.prefix = prefix;
    this.suffix =
        StateFetchingIterators.readAllAndDecodeStartingFrom(
            Caches.subCache(cache, stateKey), beamFnStateClient, request, elemCoder);
    this.elemCoder = elemCoder;
  }

  @SuppressWarnings("nullness")
  private static class WrappedObservingIterator<T> extends ElementByteSizeObservableIterator<T> {
    private final Iterator<T> wrappedIterator;
    private final org.apache.beam.sdk.coders.Coder<T> elementCoder;

    // Logically final and non-null but initialized after construction by factory method for
    // initialization ordering.
    private ElementByteSizeObserver observerProxy = null;

    private boolean observerNeedsAdvance = false;
    private boolean exceptionLogged = false;

    static <T> WrappedObservingIterator<T> create(
        Iterator<T> iterator, org.apache.beam.sdk.coders.Coder<T> elementCoder) {
      WrappedObservingIterator<T> result = new WrappedObservingIterator<>(iterator, elementCoder);
      result.observerProxy =
          new ElementByteSizeObserver() {
            @Override
            protected void reportElementSize(long elementByteSize) {
              result.notifyValueReturned(elementByteSize);
            }
          };
      return result;
    }

    private WrappedObservingIterator(
        Iterator<T> iterator, org.apache.beam.sdk.coders.Coder<T> elementCoder) {
      this.wrappedIterator = iterator;
      this.elementCoder = elementCoder;
    }

    @Override
    public boolean hasNext() {
      if (observerNeedsAdvance) {
        observerProxy.advance();
        observerNeedsAdvance = false;
      }
      return wrappedIterator.hasNext();
    }

    @Override
    public T next() {
      T value = wrappedIterator.next();
      try {
        elementCoder.registerByteSizeObserver(value, observerProxy);
        if (observerProxy.getIsLazy()) {
          // The observer will only be notified of bytes as the result
          // is used. We defer advancing the observer until hasNext in an
          // attempt to capture those bytes.
          observerNeedsAdvance = true;
        } else {
          observerNeedsAdvance = false;
          observerProxy.advance();
        }
      } catch (Exception e) {
        if (!exceptionLogged) {
          LOG.warn("Lazily observed byte size will be under reported due to exception", e);
          exceptionLogged = true;
        }
      }
      return value;
    }

    @Override
    public void remove() {
      super.remove();
    }
  }

  @Override
  protected ElementByteSizeObservableIterator<T> createIterator() {
    return WrappedObservingIterator.create(
        PrefetchableIterators.concat(prefix.iterator(), suffix.iterator()), elemCoder);
  }

  protected Object writeReplace() throws ObjectStreamException {
    return ImmutableList.copyOf(this);
  }

  /**
   * Decodes an {@link Iterable} that might be backed by state. If the terminator at the end of the
   * value stream is {@code -1} then we return a {@link StateBackedIterable} otherwise we return an
   * {@link Iterable}.
   */
  public static class Coder<T> extends IterableLikeCoder<T, Iterable<T>> {

    private final Supplier<Cache<?, ?>> cache;
    private final BeamFnStateClient beamFnStateClient;
    private final Supplier<String> instructionId;

    public Coder(
        Supplier<Cache<?, ?>> cache,
        BeamFnStateClient beamFnStateClient,
        Supplier<String> instructionId,
        org.apache.beam.sdk.coders.Coder<T> elemCoder) {
      super(elemCoder, "StateBackedIterable");
      this.cache = cache;
      this.beamFnStateClient = beamFnStateClient;
      this.instructionId = instructionId;
    }

    @Override
    protected Iterable<T> decodeToIterable(List<T> decodedElements) {
      return decodedElements;
    }

    @Override
    protected Iterable<T> decodeToIterable(
        List<T> decodedElements, long terminatorValue, InputStream in) throws IOException {
      if (terminatorValue == -1L) {
        long tokenLength = VarInt.decodeLong(in);
        ByteString token = ByteString.readFrom(ByteStreams.limit(in, tokenLength));
        return new StateBackedIterable<>(
            cache.get(),
            beamFnStateClient,
            instructionId.get(),
            StateKey.newBuilder().setRunner(StateKey.Runner.newBuilder().setKey(token)).build(),
            getElemCoder(),
            decodedElements);
      } else {
        throw new IllegalStateException(
            String.format(
                "StateBackedIterable expected terminator of 0 or -1 but received %s.",
                terminatorValue));
      }
    }

    @Override
    public void encode(Iterable<T> iterable, OutputStream outStream) throws IOException {
      if (!(iterable instanceof StateBackedIterable)) {
        super.encode(iterable, outStream);
        return;
      }

      StateBackedIterable<T> stateBackedIterable = (StateBackedIterable<T>) iterable;

      DataOutputStream dataOutStream = new DataOutputStream(outStream);
      // We don't know the size without traversing it so use a fixed size buffer
      // and encode as many elements as possible into it before outputting the size followed
      // by the elements.
      dataOutStream.writeInt(-1);
      BufferedElementCountingOutputStream countingOutputStream =
          new BufferedElementCountingOutputStream(dataOutStream, -1L);
      // Encode only the prefix
      for (T elem : stateBackedIterable.prefix) {
        countingOutputStream.markElementStart();
        getElemCoder().encode(elem, countingOutputStream);
      }
      countingOutputStream.finish();
      // Make sure all our output gets pushed to the underlying outStream.
      dataOutStream.flush();

      // Append 'len(token) token' after the -1 stream terminator.
      VarInt.encode(
          stateBackedIterable.request.getStateKey().getRunner().getKey().size(), outStream);
      stateBackedIterable.request.getStateKey().getRunner().getKey().writeTo(outStream);
    }
  }

  /** Additional parameters required by the {@link StateBackedIterable.Coder}. */
  public interface StateBackedIterableTranslationContext extends TranslationContext {
    Supplier<Cache<?, ?>> getCache();

    BeamFnStateClient getStateClient();

    Supplier<String> getCurrentInstructionId();
  }

  /** A {@link CoderTranslatorRegistrar} for {@code beam:coder:state_backed_iterable:v1}. */
  @AutoService(CoderTranslatorRegistrar.class)
  public static class Registrar implements CoderTranslatorRegistrar {

    @Override
    public Map<Class<? extends org.apache.beam.sdk.coders.Coder>, String> getCoderURNs() {
      return Collections.singletonMap(
          StateBackedIterable.Coder.class, STATE_BACKED_ITERABLE_CODER_URN);
    }

    @Override
    public Map<
            Class<? extends org.apache.beam.sdk.coders.Coder>,
            CoderTranslator<? extends org.apache.beam.sdk.coders.Coder>>
        getCoderTranslators() {
      return ImmutableMap.of(StateBackedIterable.Coder.class, new Translator());
    }
  }

  /**
   * A {@link CoderTranslator} for {@code beam:coder:state_backed_iterable:v1}.
   *
   * <p>One must supply a {@link StateBackedIterableTranslationContext} during {@link
   * CoderTranslator#fromComponents} to be able to successfully create an instance.
   */
  private static class Translator implements CoderTranslator<StateBackedIterable.Coder<?>> {

    @Override
    public List<? extends org.apache.beam.sdk.coders.Coder<?>> getComponents(
        StateBackedIterable.Coder<?> from) {
      return Collections.<org.apache.beam.sdk.coders.Coder<?>>singletonList(from.getElemCoder());
    }

    @Override
    public StateBackedIterable.Coder<?> fromComponents(
        List<org.apache.beam.sdk.coders.Coder<?>> components,
        byte[] payload,
        TranslationContext context) {
      if (context instanceof StateBackedIterableTranslationContext) {
        return new StateBackedIterable.Coder<>(
            ((StateBackedIterableTranslationContext) context).getCache(),
            ((StateBackedIterableTranslationContext) context).getStateClient(),
            ((StateBackedIterableTranslationContext) context).getCurrentInstructionId(),
            Iterables.getOnlyElement(components));
      } else {
        throw new IllegalStateException(
            String.format(
                "Unable to construct coder %s. Expected translation context %s but received %s.",
                STATE_BACKED_ITERABLE_CODER_URN,
                StateBackedIterableTranslationContext.class.getName(),
                context.getClass().getName()));
      }
    }
  }
}
