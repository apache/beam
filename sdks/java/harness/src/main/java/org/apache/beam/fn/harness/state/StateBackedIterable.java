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
import java.io.OutputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.runners.core.construction.CoderTranslation.TranslationContext;
import org.apache.beam.runners.core.construction.CoderTranslator;
import org.apache.beam.runners.core.construction.CoderTranslatorRegistrar;
import org.apache.beam.sdk.coders.IterableLikeCoder;
import org.apache.beam.sdk.fn.stream.DataStreams;
import org.apache.beam.sdk.util.BufferedElementCountingOutputStream;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.ByteStreams;

/**
 * A {@link BeamFnStateClient state} backed iterable which allows for fetching elements over the
 * portability state API. See <a
 * href="https://s.apache.org/beam-fn-state-api-and-bundle-processing">remote references</a> for
 * additional details.
 *
 * <p>One must supply a {@link StateBackedIterableTranslationContext} when using {@link
 * CoderTranslator#fromComponents} to be able to create a {@link StateBackedIterable.Coder}.
 */
public class StateBackedIterable<T> implements Iterable<T> {

  private final BeamFnStateClient beamFnStateClient;
  private final org.apache.beam.sdk.coders.Coder<T> elemCoder;
  @VisibleForTesting final StateRequest request;
  @VisibleForTesting final List<T> prefix;

  public StateBackedIterable(
      BeamFnStateClient beamFnStateClient,
      String instructionId,
      ByteString runnerKey,
      org.apache.beam.sdk.coders.Coder<T> elemCoder,
      List<T> prefix) {
    this.beamFnStateClient = beamFnStateClient;
    this.elemCoder = elemCoder;

    StateRequest.Builder requestBuilder = StateRequest.newBuilder();
    requestBuilder
        .setInstructionId(instructionId)
        .getStateKeyBuilder()
        .getRunnerBuilder()
        .setKey(runnerKey);
    this.request = requestBuilder.build();
    this.prefix = prefix;
  }

  @Override
  public Iterator<T> iterator() {
    return Iterators.concat(
        prefix.iterator(),
        new DataStreams.DataStreamDecoder(
            elemCoder,
            DataStreams.inbound(
                StateFetchingIterators.readAllStartingFrom(beamFnStateClient, request))));
  }

  /**
   * Decodes an {@link Iterable} that might be backed by state. If the terminator at the end of the
   * value stream is {@code -1} then we return a {@link StateBackedIterable} otherwise we return an
   * {@link Iterable}.
   */
  public static class Coder<T> extends IterableLikeCoder<T, Iterable<T>> {

    private final BeamFnStateClient beamFnStateClient;
    private final Supplier<String> instructionId;

    public Coder(
        BeamFnStateClient beamFnStateClient,
        Supplier<String> instructionId,
        org.apache.beam.sdk.coders.Coder<T> elemCoder) {
      super(elemCoder, "StateBackedIterable");
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
            beamFnStateClient, instructionId.get(), token, getElemCoder(), decodedElements);
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
