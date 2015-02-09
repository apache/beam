/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.transforms.join;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.runners.DirectPipeline;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner.EvaluationResults;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import org.hamcrest.Matcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Tests for CoGroupByKeyTest.  Implements Serializable for anonymous DoFns.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("serial")
public class CoGroupByKeyTest implements Serializable {

  /**
   * Converts the given list into a PCollection belonging to the provided
   * Pipeline in such a way that coder inference needs to be performed.
   */
  private PCollection<KV<Integer, String>> createInput(
      Pipeline p, List<KV<Integer, String>> list) {
    return p
            .apply(Create.of(list))
            // Create doesn't infer coders for parameterized types.
            .setCoder(
                KvCoder.of(BigEndianIntegerCoder.of(), StringUtf8Coder.of()))
            // Do a dummy transform so consumers must deal with coder inference.
            .apply(ParDo.of(new DoFn<KV<Integer, String>,
                                     KV<Integer, String>>() {
              @Override
              public void processElement(ProcessContext c) {
                c.output(c.element());
              }
            }));
  }

  /**
   * Returns a PCollection<KV<Integer, CoGbkResult>> containing the result
   * of a CoGbk over 2 PCollection<KV<Integer, String>>, where each PCollection
   * has no duplicate keys and the key sets of each PCollection are
   * intersecting but neither is a subset of the other.
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
    PCollection<KV<Integer, String>> collection1 = createInput(p, list1);
    PCollection<KV<Integer, String>> collection2 = createInput(p, list2);
    PCollection<KV<Integer, CoGbkResult>> coGbkResults =
        KeyedPCollectionTuple.of(tag1, collection1)
            .and(tag2, collection2)
            .apply(CoGroupByKey.<Integer>create());
    return coGbkResults;
  }

  @Test
  public void testCoGroupByKeyGetOnly() {
    TupleTag<String> tag1 = new TupleTag<>();
    TupleTag<String> tag2 = new TupleTag<>();

    DirectPipeline p = DirectPipeline.createForTest();

    PCollection<KV<Integer, CoGbkResult>> coGbkResults =
        buildGetOnlyGbk(p, tag1, tag2);

    EvaluationResults results = p.run();

    List<KV<Integer, CoGbkResult>> finalResult =
        results.getPCollection(coGbkResults);

    HashMap<Integer, Matcher<String>> collection1Matchers =
        new HashMap<Integer, Matcher<String>>() {
      {
        put(1, equalTo("collection1-1"));
        put(2, equalTo("collection1-2"));
      }
    };

    HashMap<Integer, Matcher<String>> collection2Matchers =
        new HashMap<Integer, Matcher<String>>() {
      {
        put(2, equalTo("collection2-2"));
        put(3, equalTo("collection2-3"));
      }
    };

    for (KV<Integer, CoGbkResult> result : finalResult) {
      int key = result.getKey();
      CoGbkResult row = result.getValue();
      checkGetOnlyForKey(key, collection1Matchers, row, tag1, "default");
      checkGetOnlyForKey(key, collection2Matchers, row, tag2, "default");
    }
  }

  /**
   * Check that a singleton value for a key in a CoGbkResult matches the
   * expected value in a map.  If no value exists for the key, check that
   * a default value is given (if supplied) and that an
   * {@link IllegalArgumentException} is thrown if no default is supplied.
   */
  private <K, V> void checkGetOnlyForKey(
      K key,
      HashMap<K, Matcher<V>> matchers,
      CoGbkResult row,
      TupleTag<V> tag,
      V defaultValue) {
    if (matchers.containsKey(key)) {
      assertThat(row.getOnly(tag), matchers.get(key));
    } else {
      assertThat(row.getOnly(tag, defaultValue), equalTo(defaultValue));
      try {
        row.getOnly(tag);
        fail();
      } catch (IllegalArgumentException e) {
        // if no value exists, an IllegalArgumentException should be thrown
      }

    }
  }

  /**
   * Returns a PCollection<KV<Integer, CoGbkResult>> containing the
   * results of the CoGbk over 3 PCollection<KV<Integer, String>>, each of
   * which correlates a customer id to purchases, addresses, or names,
   * respectively.
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
        createInput(p, idToPurchases);

    PCollection<KV<Integer, String>> addressTable =
        createInput(p, idToAddress);

    PCollection<KV<Integer, String>> nameTable =
        createInput(p, idToName);

    PCollection<KV<Integer, CoGbkResult>> coGbkResults =
        KeyedPCollectionTuple.of(namesTag, nameTable)
            .and(addressesTag, addressTable)
            .and(purchasesTag, purchasesTable)
            .apply(CoGroupByKey.<Integer>create());
    return coGbkResults;
  }

  @Test
  public void testCoGroupByKey() {
    TupleTag<String> namesTag = new TupleTag<>();
    TupleTag<String> addressesTag = new TupleTag<>();
    TupleTag<String> purchasesTag = new TupleTag<>();

    DirectPipeline p = DirectPipeline.createForTest();

    PCollection<KV<Integer, CoGbkResult>> coGbkResults =
        buildPurchasesCoGbk(p, purchasesTag, addressesTag, namesTag);

    EvaluationResults results = p.run();

    List<KV<Integer, CoGbkResult>> finalResult =
        results.getPCollection(coGbkResults);

    HashMap<Integer, Matcher<Iterable<? extends String>>> namesMatchers =
        new HashMap<Integer, Matcher<Iterable<? extends String>>>() {
      {
        put(1, containsInAnyOrder("John Smith"));
        put(2, containsInAnyOrder("Sally James"));
        put(8, containsInAnyOrder("Jeffery Spalding"));
        put(20, containsInAnyOrder("Joan Lichtfield"));
      }
    };

    HashMap<Integer, Matcher<Iterable<? extends String>>> addressesMatchers =
        new HashMap<Integer, Matcher<Iterable<? extends String>>>() {
      {
        put(2, containsInAnyOrder("53 S. 3rd"));
        put(3, containsInAnyOrder("29 School Rd"));
        put(8, containsInAnyOrder("6 Watling Rd"));
        put(10, containsInAnyOrder("383 Jackson Street"));
        put(20, containsInAnyOrder("3 W. Arizona"));
      }
    };

    HashMap<Integer, Matcher<Iterable<? extends String>>> purchasesMatchers =
        new HashMap<Integer, Matcher<Iterable<? extends String>>>() {
      {
        put(1, containsInAnyOrder("Shoes", "Book"));
        put(2, containsInAnyOrder("Suit", "Boat"));
        put(3, containsInAnyOrder("Car", "House"));
        put(4, containsInAnyOrder("Suit"));
        put(8, containsInAnyOrder("House", "Suit Case"));
        put(10, containsInAnyOrder("Pens"));
        put(11, containsInAnyOrder("House"));
        put(14, containsInAnyOrder("Shoes"));
      }
    };

    // TODO: Figure out a way to do a hamcrest matcher for CoGbkResults.
    for (KV<Integer, CoGbkResult> result : finalResult) {
      int key = result.getKey();
      CoGbkResult row = result.getValue();
      checkValuesMatch(key, namesMatchers, row, namesTag);
      checkValuesMatch(key, addressesMatchers, row, addressesTag);
      checkValuesMatch(key, purchasesMatchers, row, purchasesTag);

    }

  }

  /**
   * Checks that the values for the given tag in the given row matches the
   * expected values for the given key in the given matchers map.
   */
  private <K, V> void checkValuesMatch(
      K key,
      HashMap<K, Matcher<Iterable<? extends V>>> matchers,
      CoGbkResult row,
      TupleTag<V> tag) {
    Iterable<V> taggedValues = row.getAll(tag);
    if (taggedValues.iterator().hasNext()) {
      assertThat(taggedValues, matchers.get(key));
    } else {
      assertNull(matchers.get(key));
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

    @Override
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
      ArrayList<String> addressList = new ArrayList<String>();
      for (String address : addresses) {
        addressList.add(address);
      }

      Iterable<String> purchases = row.getAll(purchasesTag);

      int purchaseCount = 0;
      for (String purchase : purchases) {
        purchaseCount++;
      }

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
  public void testConsumingDoFn() {
    TupleTag<String> purchasesTag = new TupleTag<>();
    TupleTag<String> addressesTag = new TupleTag<>();
    TupleTag<String> namesTag = new TupleTag<>();

    // result1 should get filtered out because it has a name.
    CoGbkResult result1 = CoGbkResult
        .of(purchasesTag, Arrays.asList("3a", "3b"))
        .and(addressesTag, Arrays.asList("2a", "2b"))
        .and(namesTag, Arrays.asList("1a"));
    // result 2 should be counted because it has an address and purchases.
    CoGbkResult result2 = CoGbkResult
        .of(purchasesTag, Arrays.asList("5a", "5b"))
        .and(addressesTag, Arrays.asList("4a"))
        .and(namesTag, new ArrayList<String>());
    // result 3 should not be counted because it has no addresses.
    CoGbkResult result3 = CoGbkResult
        .of(purchasesTag, Arrays.asList("7a", "7b"))
        .and(addressesTag, new ArrayList<String>())
        .and(namesTag, new ArrayList<String>());
    // result 4 should be counted as 0, because it has no purchases.
    CoGbkResult result4 = CoGbkResult
        .of(purchasesTag, new ArrayList<String>())
        .and(addressesTag, Arrays.asList("8a"))
        .and(namesTag, new ArrayList<String>());

    List<KV<String, Integer>> results =
        DoFnTester.of(
            new CorrelatePurchaseCountForAddressesWithoutNamesFn(
                purchasesTag,
                addressesTag,
                namesTag))
                .processBatch(
                    KV.of(1, result1),
                    KV.of(2, result2),
                    KV.of(3, result3),
                    KV.of(4, result4));
    assertThat(results, containsInAnyOrder(KV.of("4a", 2), KV.of("8a", 0)));
  }

  /**
   * Tests the pipeline end-to-end.  Builds the purchases CoGroupByKey, and
   * applies CorrelatePurchaseCountForAddressesWithoutNamesFn to the results.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testCoGroupByKeyHandleResults() {
    TupleTag<String> namesTag = new TupleTag<>();
    TupleTag<String> addressesTag = new TupleTag<>();
    TupleTag<String> purchasesTag = new TupleTag<>();

    Pipeline p = TestPipeline.create();

    PCollection<KV<Integer, CoGbkResult>> coGbkResults =
        buildPurchasesCoGbk(p, purchasesTag, addressesTag, namesTag);

    // Do some simple processing on the result of the CoGroupByKey.  Count the
    // purchases for each address on record that has no associated name.
    PCollection<KV<String, Integer>>
      purchaseCountByKnownAddressesWithoutKnownNames =
        coGbkResults.apply(ParDo.of(
            new CorrelatePurchaseCountForAddressesWithoutNamesFn(
                purchasesTag, addressesTag, namesTag)));

    DataflowAssert.that(purchaseCountByKnownAddressesWithoutKnownNames)
        .containsInAnyOrder(
            KV.of("29 School Rd", 2),
            KV.of("383 Jackson Street", 1));
    p.run();
  }
}
