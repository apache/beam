package org.apache.beam.runners.jstorm.translation.translator;

import org.apache.beam.runners.jstorm.StormPipelineOptions;
import org.apache.beam.runners.jstorm.TestJStormRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(JUnit4.class)
public class CoGroupByKeyTest implements Serializable {
    /**
     * Converts the given list into a PCollection belonging to the provided
     * Pipeline in such a way that coder inference needs to be performed.
     */
    private PCollection<KV<Integer, String>> createInput(String name,
                                                         Pipeline p, List<KV<Integer, String>> list) {
        return createInput(name, p, list,  new ArrayList<Long>());
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
                        .apply(CoGroupByKey.<Integer>create());
        return coGbkResults;
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testCoGroupByKeyGetOnly() {
        StormPipelineOptions options = PipelineOptionsFactory.as(StormPipelineOptions.class);
        options.setRunner(TestJStormRunner.class);
        options.setLocalMode(true);

        Pipeline p = Pipeline.create(options);

        final TupleTag<String> tag1 = new TupleTag<>();
        final TupleTag<String> tag2 = new TupleTag<>();

        PCollection<KV<Integer, CoGbkResult>> coGbkResults =
                buildGetOnlyGbk(p, tag1, tag2);

        PAssert.thatMap(coGbkResults).satisfies(
                new SerializableFunction<Map<Integer, CoGbkResult>, Void>() {
                    @Override
                    public Void apply(Map<Integer, CoGbkResult> results) {
                        assertEquals("collection1-1", results.get(1).getOnly(tag1));
                        assertEquals("collection1-2", results.get(2).getOnly(tag1));
                        assertEquals("collection2-2", results.get(2).getOnly(tag2));
                        assertEquals("collection2-3", results.get(3).getOnly(tag2));
                        return null;
                    }
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
                        .apply(CoGroupByKey.<Integer>create());
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
                        .apply(CoGroupByKey.<Integer>create());
        return coGbkResults;
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testCoGroupByKey() {
        StormPipelineOptions options = PipelineOptionsFactory.as(StormPipelineOptions.class);
        options.setRunner(TestJStormRunner.class);
        options.setLocalMode(true);

        Pipeline p = Pipeline.create(options);

        final TupleTag<String> namesTag = new TupleTag<>();
        final TupleTag<String> addressesTag = new TupleTag<>();
        final TupleTag<String> purchasesTag = new TupleTag<>();


        PCollection<KV<Integer, CoGbkResult>> coGbkResults =
                buildPurchasesCoGbk(p, purchasesTag, addressesTag, namesTag);

        PAssert.thatMap(coGbkResults).satisfies(
                new SerializableFunction<Map<Integer, CoGbkResult>, Void>() {
                    @Override
                    public Void apply(Map<Integer, CoGbkResult> results) {
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
                    }
                });

        p.run();
    }
}