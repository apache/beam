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
package org.apache.beam.sdk.transforms.join;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for CoGroupByKeyTest.  Implements Serializable for anonymous DoFns.
 */
@RunWith(JUnit4.class)
public class CoGroupByKeyTest implements Serializable {

  /**
   * Converts the given list into a PCollection belonging to the provided
   * Pipeline in such a way that coder inference needs to be performed.
   */
  private PCollection<KV<Integer, String>> createInput(String name,
      Pipeline p, List<KV<Integer, String>> list) {
    return createInput(name, p, list, new ArrayList<>());
  }

  /**
   * Converts the given list with timestamps into a PCollection.
   */
  private PCollection<KV<Integer, String>> createInput(String name,
      Pipeline p, List<KV<Integer, String>> list, List<Long> timestamps) {
    PCollection<KV<Integer, String>> input;
    if (timestamps.isEmpty()) {
      input = p.apply("Create" + name, Create.of(list)
          .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), StringUtf8Coder.of())));
    } else {
      input = p.apply("Create" + name, Create.timestamped(list, timestamps)
          .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), StringUtf8Coder.of())));
    }
    return input.apply(
        "Identity" + name,
        ParDo.of(
            new DoFn<KV<Integer, String>, KV<Integer, String>>() {
              @ProcessElement
              public void processElement(ProcessContext c) {
                c.output(c.element());
              }
            }));
  }

  /**
   * Returns a {@code PCollection<KV<Integer, CoGbkResult>>} containing the result
   * of a {@link CoGroupByKey} over 2 {@code PCollection<KV<Integer, String>>},
   * where each {@link PCollection} has no duplicate keys and the key sets of
   * each {@link PCollection} are intersecting but neither is a subset of the other.
   */
  private PCollection<KV<Integer, CoGbkResult>> buildGetOnlyGbk(
      Pipeline p,
      TupleTag<String> tag1,
      TupleTag<String> tag2) {
    List<KV<Integer, String>> list1 =
        Arrays.asList(
            KV.of(1, "collection1-1"),
            KV.of(2, "collection1-2"));
    List<KV<Integer, String>> list2 =
        Arrays.asList(
            KV.of(2, "collection2-2"),
            KV.of(3, "collection2-3"));
    PCollection<KV<Integer, String>> collection1 = createInput("CreateList1", p, list1);
    PCollection<KV<Integer, String>> collection2 = createInput("CreateList2", p, list2);
    PCollection<KV<Integer, CoGbkResult>> coGbkResults =
        KeyedPCollectionTuple.of(tag1, collection1)
            .and(tag2, collection2)
            .apply(CoGroupByKey.create());
    return coGbkResults;
  }

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Test
  @Category(ValidatesRunner.class)
  public void testCoGroupByKeyGetOnly() {
    final TupleTag<String> tag1 = new TupleTag<>();
    final TupleTag<String> tag2 = new TupleTag<>();

    PCollection<KV<Integer, CoGbkResult>> coGbkResults =
        buildGetOnlyGbk(p, tag1, tag2);

    PAssert.thatMap(coGbkResults)
        .satisfies(
            results -> {
              assertEquals("collection1-1", results.get(1).getOnly(tag1));
              assertEquals("collection1-2", results.get(2).getOnly(tag1));
              assertEquals("collection2-2", results.get(2).getOnly(tag2));
              assertEquals("collection2-3", results.get(3).getOnly(tag2));
              return null;
            });

    p.run();
  }

  /**
   * Returns a {@code PCollection<KV<Integer, CoGbkResult>>} containing the
   * results of the {@code CoGroupByKey} over three
   * {@code PCollection<KV<Integer, String>>}, each of which correlates
   * a customer id to purchases, addresses, or names, respectively.
   */
  private PCollection<KV<Integer, CoGbkResult>> buildPurchasesCoGbk(
      Pipeline p,
      TupleTag<String> purchasesTag,
      TupleTag<String> addressesTag,
      TupleTag<String> namesTag) {
    List<KV<Integer, String>> idToPurchases =
        Arrays.asList(
            KV.of(2, "Boat"),
            KV.of(1, "Shoes"),
            KV.of(3, "Car"),
            KV.of(1, "Book"),
            KV.of(10, "Pens"),
            KV.of(8, "House"),
            KV.of(4, "Suit"),
            KV.of(11, "House"),
            KV.of(14, "Shoes"),
            KV.of(2, "Suit"),
            KV.of(8, "Suit Case"),
            KV.of(3, "House"));

    List<KV<Integer, String>> idToAddress =
        Arrays.asList(
            KV.of(2, "53 S. 3rd"),
            KV.of(10, "383 Jackson Street"),
            KV.of(20, "3 W. Arizona"),
            KV.of(3, "29 School Rd"),
            KV.of(8, "6 Watling Rd"));

    List<KV<Integer, String>> idToName =
        Arrays.asList(
            KV.of(1, "John Smith"),
            KV.of(2, "Sally James"),
            KV.of(8, "Jeffery Spalding"),
            KV.of(20, "Joan Lichtfield"));

    PCollection<KV<Integer, String>> purchasesTable =
        createInput("CreateIdToPurchases", p, idToPurchases);

    PCollection<KV<Integer, String>> addressTable =
        createInput("CreateIdToAddress", p, idToAddress);

    PCollection<KV<Integer, String>> nameTable =
        createInput("CreateIdToName", p, idToName);

    PCollection<KV<Integer, CoGbkResult>> coGbkResults =
        KeyedPCollectionTuple.of(namesTag, nameTable)
            .and(addressesTag, addressTable)
            .and(purchasesTag, purchasesTable)
            .apply(CoGroupByKey.create());
    return coGbkResults;
  }

  /**
   * Returns a {@code PCollection<KV<Integer, CoGbkResult>>} containing the
   * results of the {@code CoGroupByKey} over 2 {@code PCollection<KV<Integer, String>>},
   * each of which correlates a customer id to clicks, purchases, respectively.
   */
  private PCollection<KV<Integer, CoGbkResult>> buildPurchasesCoGbkWithWindowing(
      Pipeline p,
      TupleTag<String> clicksTag,
      TupleTag<String> purchasesTag) {
    List<KV<Integer, String>> idToClick =
        Arrays.asList(
            KV.of(1, "Click t0"),
            KV.of(2, "Click t2"),
            KV.of(1, "Click t4"),
            KV.of(1, "Click t6"),
            KV.of(2, "Click t8"));

    List<KV<Integer, String>> idToPurchases =
        Arrays.asList(
            KV.of(1, "Boat t1"),
            KV.of(1, "Shoesi t2"),
            KV.of(1, "Pens t3"),
            KV.of(2, "House t4"),
            KV.of(2, "Suit t5"),
            KV.of(1, "Car t6"),
            KV.of(1, "Book t7"),
            KV.of(2, "House t8"),
            KV.of(2, "Shoes t9"),
            KV.of(2, "House t10"));

    PCollection<KV<Integer, String>> clicksTable =
        createInput("CreateClicks",
            p,
            idToClick,
            Arrays.asList(0L, 2L, 4L, 6L, 8L))
        .apply("WindowClicks", Window.<KV<Integer, String>>into(
            FixedWindows.of(new Duration(4)))
            .withTimestampCombiner(TimestampCombiner.EARLIEST));

    PCollection<KV<Integer, String>> purchasesTable =
        createInput("CreatePurchases",
            p,
            idToPurchases,
            Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L))
        .apply("WindowPurchases", Window.<KV<Integer, String>>into(
            FixedWindows.of(new Duration(4)))
            .withTimestampCombiner(TimestampCombiner.EARLIEST));

    PCollection<KV<Integer, CoGbkResult>> coGbkResults =
        KeyedPCollectionTuple.of(clicksTag, clicksTable)
            .and(purchasesTag, purchasesTable)
            .apply(CoGroupByKey.create());
    return coGbkResults;
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testCoGroupByKey() {
    final TupleTag<String> namesTag = new TupleTag<>();
    final TupleTag<String> addressesTag = new TupleTag<>();
    final TupleTag<String> purchasesTag = new TupleTag<>();


    PCollection<KV<Integer, CoGbkResult>> coGbkResults =
        buildPurchasesCoGbk(p, purchasesTag, addressesTag, namesTag);

    PAssert.thatMap(coGbkResults)
        .satisfies(
            results -> {
              CoGbkResult result1 = results.get(1);
              assertEquals("John Smith", result1.getOnly(namesTag));
              assertThat(result1.getAll(purchasesTag), containsInAnyOrder("Shoes", "Book"));

              CoGbkResult result2 = results.get(2);
              assertEquals("Sally James", result2.getOnly(namesTag));
              assertEquals("53 S. 3rd", result2.getOnly(addressesTag));
              assertThat(result2.getAll(purchasesTag), containsInAnyOrder("Suit", "Boat"));

              CoGbkResult result3 = results.get(3);
              assertEquals("29 School Rd", result3.getOnly(addressesTag), "29 School Rd");
              assertThat(result3.getAll(purchasesTag), containsInAnyOrder("Car", "House"));

              CoGbkResult result8 = results.get(8);
              assertEquals("Jeffery Spalding", result8.getOnly(namesTag));
              assertEquals("6 Watling Rd", result8.getOnly(addressesTag));
              assertThat(result8.getAll(purchasesTag), containsInAnyOrder("House", "Suit Case"));

              CoGbkResult result20 = results.get(20);
              assertEquals("Joan Lichtfield", result20.getOnly(namesTag));
              assertEquals("3 W. Arizona", result20.getOnly(addressesTag));

              assertEquals("383 Jackson Street", results.get(10).getOnly(addressesTag));

              assertThat(results.get(4).getAll(purchasesTag), containsInAnyOrder("Suit"));
              assertThat(results.get(10).getAll(purchasesTag), containsInAnyOrder("Pens"));
              assertThat(results.get(11).getAll(purchasesTag), containsInAnyOrder("House"));
              assertThat(results.get(14).getAll(purchasesTag), containsInAnyOrder("Shoes"));

              return null;
            });

    p.run();
  }

  /**
   * A DoFn used in testCoGroupByKeyWithWindowing(), to test processing the results of a
   * CoGroupByKey.
   */
  private static class ClickOfPurchaseFn
      extends DoFn<KV<Integer, CoGbkResult>, KV<String, String>> {
    private final TupleTag<String> clicksTag;

    private final TupleTag<String> purchasesTag;

    private ClickOfPurchaseFn(
        TupleTag<String> clicksTag,
        TupleTag<String> purchasesTag) {
      this.clicksTag = clicksTag;
      this.purchasesTag = purchasesTag;
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) {
      BoundedWindow w = window;
      KV<Integer, CoGbkResult> e = c.element();
      CoGbkResult row = e.getValue();
      Iterable<String> clicks = row.getAll(clicksTag);
      Iterable<String> purchases = row.getAll(purchasesTag);
      for (String click : clicks) {
        for (String purchase : purchases) {
          c.output(KV.of(click + ":" + purchase,
                         c.timestamp().getMillis() + ":" + w.maxTimestamp().getMillis()));
        }
      }
    }
  }


  /**
   * A DoFn used in testCoGroupByKeyHandleResults(), to test processing the
   * results of a CoGroupByKey.
   */
  private static class CorrelatePurchaseCountForAddressesWithoutNamesFn extends
      DoFn<KV<Integer, CoGbkResult>, KV<String, Integer>> {
    private final TupleTag<String> purchasesTag;

    private final TupleTag<String> addressesTag;

    private final TupleTag<String> namesTag;

    private CorrelatePurchaseCountForAddressesWithoutNamesFn(
        TupleTag<String> purchasesTag,
        TupleTag<String> addressesTag,
        TupleTag<String> namesTag) {
      this.purchasesTag = purchasesTag;
      this.addressesTag = addressesTag;
      this.namesTag = namesTag;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      KV<Integer, CoGbkResult> e = c.element();
      CoGbkResult row = e.getValue();
      // Don't actually care about the id.
      Iterable<String> names = row.getAll(namesTag);
      if (names.iterator().hasNext()) {
        // Nothing to do. There was a name.
        return;
      }
      Iterable<String> addresses = row.getAll(addressesTag);
      if (!addresses.iterator().hasNext()) {
        // Nothing to do, there was no address.
        return;
      }
      // Buffer the addresses so we can accredit all of them with
      // corresponding purchases. All addresses are for the same id, so
      // if there are multiple, we apply the same purchase count to all.
      ArrayList<String> addressList = new ArrayList<>();
      for (String address : addresses) {
        addressList.add(address);
      }

      Iterable<String> purchases = row.getAll(purchasesTag);

      int purchaseCount = Iterables.size(purchases);

      for (String address : addressList) {
        c.output(KV.of(address, purchaseCount));
      }
    }
  }

  /**
   * Tests that the consuming DoFn
   * (CorrelatePurchaseCountForAddressesWithoutNamesFn) performs as expected.
   */
  @SuppressWarnings("unchecked")
  @Test
  @Category(NeedsRunner.class)
  public void testConsumingDoFn() throws Exception {
    TupleTag<String> purchasesTag = new TupleTag<>();
    TupleTag<String> addressesTag = new TupleTag<>();
    TupleTag<String> namesTag = new TupleTag<>();

    // result1 should get filtered out because it has a name.
    CoGbkResult result1 = CoGbkResult
        .of(purchasesTag, Arrays.asList("3a", "3b"))
        .and(addressesTag, Arrays.asList("2a", "2b"))
        .and(namesTag, Arrays.asList("1a"));
    // result 2 should be counted because it has an address and purchases.
    CoGbkResult result2 =
        CoGbkResult.of(purchasesTag, Arrays.asList("5a", "5b"))
            .and(addressesTag, Arrays.asList("4a"))
            .and(namesTag, new ArrayList<>());
    // result 3 should not be counted because it has no addresses.
    CoGbkResult result3 =
        CoGbkResult.of(purchasesTag, Arrays.asList("7a", "7b"))
            .and(addressesTag, new ArrayList<>())
            .and(namesTag, new ArrayList<>());
    // result 4 should be counted as 0, because it has no purchases.
    CoGbkResult result4 =
        CoGbkResult.of(purchasesTag, new ArrayList<>())
            .and(addressesTag, Arrays.asList("8a"))
            .and(namesTag, new ArrayList<>());

    KvCoder<Integer, CoGbkResult> coder = KvCoder.of(
        VarIntCoder.of(),
        CoGbkResult.CoGbkResultCoder.of(
            CoGbkResultSchema.of(
                ImmutableList.of(purchasesTag, addressesTag, namesTag)),
            UnionCoder.of(
                ImmutableList.of(
                    StringUtf8Coder.of(),
                    StringUtf8Coder.of(),
                    StringUtf8Coder.of()))));

    PCollection<KV<String, Integer>> results =
        p.apply(
                Create.of(
                        KV.of(1, result1), KV.of(2, result2), KV.of(3, result3), KV.of(4, result4))
                    .withCoder(coder))
            .apply(
                ParDo.of(
                    new CorrelatePurchaseCountForAddressesWithoutNamesFn(
                        purchasesTag, addressesTag, namesTag)));

    PAssert.that(results).containsInAnyOrder(KV.of("4a", 2), KV.of("8a", 0));

    p.run();
  }

  /**
   * Tests the pipeline end-to-end.  Builds the purchases CoGroupByKey, and
   * applies CorrelatePurchaseCountForAddressesWithoutNamesFn to the results.
   */
  @SuppressWarnings("unchecked")
  @Test
  @Category(ValidatesRunner.class)
  public void testCoGroupByKeyHandleResults() {
    TupleTag<String> namesTag = new TupleTag<>();
    TupleTag<String> addressesTag = new TupleTag<>();
    TupleTag<String> purchasesTag = new TupleTag<>();

    PCollection<KV<Integer, CoGbkResult>> coGbkResults =
        buildPurchasesCoGbk(p, purchasesTag, addressesTag, namesTag);

    // Do some simple processing on the result of the CoGroupByKey.  Count the
    // purchases for each address on record that has no associated name.
    PCollection<KV<String, Integer>>
      purchaseCountByKnownAddressesWithoutKnownNames =
        coGbkResults.apply(ParDo.of(
            new CorrelatePurchaseCountForAddressesWithoutNamesFn(
                purchasesTag, addressesTag, namesTag)));

    PAssert.that(purchaseCountByKnownAddressesWithoutKnownNames)
        .containsInAnyOrder(
            KV.of("29 School Rd", 2),
            KV.of("383 Jackson Street", 1));
    p.run();
  }

  /**
   * Tests the pipeline end-to-end with FixedWindows.
   */
  @SuppressWarnings("unchecked")
  @Test
  @Category(ValidatesRunner.class)
  public void testCoGroupByKeyWithWindowing() {
    TupleTag<String> clicksTag = new TupleTag<>();
    TupleTag<String> purchasesTag = new TupleTag<>();

    PCollection<KV<Integer, CoGbkResult>> coGbkResults =
        buildPurchasesCoGbkWithWindowing(p, clicksTag, purchasesTag);

    PCollection<KV<String, String>>
        clickOfPurchase = coGbkResults.apply(ParDo.of(
            new ClickOfPurchaseFn(clicksTag, purchasesTag)));
    PAssert.that(clickOfPurchase)
        .containsInAnyOrder(
            KV.of("Click t0:Boat t1", "0:3"),
            KV.of("Click t0:Shoesi t2", "0:3"),
            KV.of("Click t0:Pens t3", "0:3"),
            KV.of("Click t4:Car t6", "4:7"),
            KV.of("Click t4:Book t7", "4:7"),
            KV.of("Click t6:Car t6", "4:7"),
            KV.of("Click t6:Book t7", "4:7"),
            KV.of("Click t8:House t8", "8:11"),
            KV.of("Click t8:Shoes t9", "8:11"),
            KV.of("Click t8:House t10", "8:11"));
    p.run();
  }
}
