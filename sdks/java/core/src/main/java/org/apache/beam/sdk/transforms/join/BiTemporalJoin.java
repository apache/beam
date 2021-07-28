package org.apache.beam.sdk.transforms.join;

import avro.shaded.com.google.common.collect.ImmutableList;
import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class BiTemporalJoin {

  public static <K, V1, V2> Impl<K, V1, V2> of(
          PCollection<KV<K, V2>> secondCollection) {
    return new AutoValue_BiTemporalJoin_Impl.Builder<K, V1, V2>()
        .setSecondCollection(secondCollection)
        .setFirstCollectionDelay(Duration.ZERO)
        .setSecondCollectionDelay(Duration.ZERO)
        .build();
  }

  @AutoValue
  public abstract static class Impl<K, V1, V2>
      extends PTransform<PCollection<KV<K, V1>>, PCollection<KV<K, PairResult<V1, V2>>>> {
    abstract PCollection<KV<K, V2>> getSecondCollection();

    abstract Duration getFirstCollectionDelay();

    abstract Duration getSecondCollectionDelay();

    abstract Builder<K, V1, V2> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K, V1, V2> {
      abstract Builder<K, V1, V2> setSecondCollection(
          PCollection<KV<K, V2>> rhsCollection);

      public abstract Builder<K, V1, V2> setFirstCollectionDelay(Duration value);

      public abstract Builder<K, V1, V2> setSecondCollectionDelay(Duration value);

      abstract Impl<K, V1, V2> build();
    }

    public Impl<K, V1, V2> asOf() {
      return toBuilder().build();
    }

    @Override
    public PCollection<KV<K, PairResult<V1, V2>>> expand(PCollection<KV<K, V1>> input) {
      Coder<K> keyCoder = JoinUtils.getKeyCoder(input);
      Coder<V1> firstValueCoder = JoinUtils.getValueCoder(input);
      Coder<V2> secondValueCoder = JoinUtils.getValueCoder(getSecondCollection());
      UnionCoder unionCoder = UnionCoder.of(ImmutableList.of(firstValueCoder, secondValueCoder));
      KvCoder<K, RawUnionValue> kvCoder = KvCoder.of(JoinUtils.getKeyCoder(input), unionCoder);
      PCollectionList<KV<K, RawUnionValue>> union =
          PCollectionList.of(JoinUtils.makeUnionTable(0, input, kvCoder))
              .and(JoinUtils.makeUnionTable(1, getSecondCollection(), kvCoder));
      return union
          .apply("Flatten", Flatten.pCollections())
          .apply(
              "Join",
              ParDo.of(
                  new BiTemporalJoinDoFn<>(
                      firstValueCoder,
                      secondValueCoder,
                      getFirstCollectionDelay(),
                      getSecondCollectionDelay())))
          .setCoder(KvCoder.of(keyCoder, PairResultCoder.<V1, V2>of(firstValueCoder, secondValueCoder)));
    }
  }


  private static class BiTemporalJoinDoFn<K, V1, V2> extends DoFn<KV<K, RawUnionValue>, KV<K, PairResult<V1, V2>>> {
    private static final int FIRST_TAG = 0;
    private static final int SECOND_TAG = 1;

    private final Coder<V1> firstValueCoder;
    private final Coder<V2> secondValueCoder;

    private final Duration firstElementDelay;
    private final Duration secondElementDelay;

    @StateId("v1Items")
    private final StateSpec<OrderedListState<V1>> firstItems;

    @StateId("v2Items")
    private final StateSpec<OrderedListState<V2>> secondItems;

    public BiTemporalJoinDoFn(Coder<V1> firstValueCoder,
                              Coder<V2> secondValueCoder,
                              Duration firstElementDelay,
                              Duration secondElementDelay) {
      this.firstValueCoder = firstValueCoder;
      this.secondValueCoder = secondValueCoder;
      this.firstElementDelay = firstElementDelay;
      this.secondElementDelay = secondElementDelay;
      firstItems = StateSpecs.orderedList(firstValueCoder);
      secondItems = StateSpecs.orderedList(secondValueCoder);
    }

    @ProcessElement
    public void process(ProcessContext context,
                        @Element KV<K, RawUnionValue> element,
                        @Timestamp Instant ts,
                        @StateId("v1Items") OrderedListState<V1> firstItems,
                        @StateId("v2Items") OrderedListState<V2> secondItems
    ) {
      switch (element.getValue().getUnionTag()) {
        case FIRST_TAG:
          firstItems.add(TimestampedValue.of((V1) element.getValue().getValue(), ts));
          for (TimestampedValue<V2> value :
              getRange(secondItems, ts, firstElementDelay, secondElementDelay)) {
            context.output(KV.of(element.getKey(), PairResult.of((V1) element.getValue().getValue(),
                                                                 value.getValue())));
          }
          break;
        case SECOND_TAG:
          secondItems.add(TimestampedValue.of((V2) element.getValue().getValue(), ts));
          for (TimestampedValue<V1> value :
              getRange(firstItems, ts, secondElementDelay, firstElementDelay)) {
            context.output(KV.of(element.getKey(), PairResult.of(value.getValue(),
                                                                 (V2) element.getValue().getValue())));
          }
      }
    }

    private <OtherV> Iterable<TimestampedValue<OtherV>> getRange(OrderedListState<OtherV> otherItems,
                                                                 Instant ts, Duration newElementDelay,
                                                                 Duration otherElementDelay) {
      Instant beginning = ts.minus(otherElementDelay);
      Instant end = beginning.plus(newElementDelay);
      return otherItems.readRange(beginning, end);
    }
  }
}


