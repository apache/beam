# CoGroupByKey

```CoGroupByKey``` performs a relational join of two or more key/value ```PCollections``` that have the same key type. Design Your Pipeline shows an example pipeline that uses a join.

Consider using ```CoGroupByKey``` if you have multiple data sets that provide information about related things. For example, let’s say you have two different files with user data: one file has names and email addresses; the other file has names and phone numbers. You can join those two data sets, using the user name as a common key and the other data as the associated values. After the join, you have one data set that contains all of the information (email addresses and phone numbers) associated with each name.

If you are using unbounded ```PCollections```, you must use either non-global windowing or an aggregation trigger in order to perform a ```CoGroupByKey```. See ```GroupByKey``` and unbounded ```PCollections``` for more details.

In the Beam SDK for Java, `CoGroupByKey` accepts a tuple of keyed PCollections (PCollection<KV<K, V>>) as input. For type safety, the SDK requires you to pass each `PCollection` as part of a ```KeyedPCollectionTuple```. You must declare a ```TupleTag``` for each input ```PCollection``` in the KeyedPCollectionTuple that you want to pass to CoGroupByKey. As output, ```CoGroupByKey``` returns a ```PCollection<KV<K, CoGbkResult>>```, which groups values from all the input PCollections by their common keys. Each key (all of type K) will have a different CoGbkResult, which is a map from TupleTag<T> to Iterable<T>. You can access a specific collection in an CoGbkResult object by using the TupleTag that you supplied with the initial collection.

The following conceptual examples use two input collections to show the mechanics of ```CoGroupByKey```.

The first set of data has a ```TupleTag<String>``` called emailsTag and contains names and email addresses. The second set of data has a ```TupleTag<String>``` called phonesTag and contains names and phone numbers.

```
final List<KV<String, String>> emailsList =
    Arrays.asList(
        KV.of("amy", "amy@example.com"),
        KV.of("carl", "carl@example.com"),
        KV.of("julia", "julia@example.com"),
        KV.of("carl", "carl@email.com"));

final List<KV<String, String>> phonesList =
    Arrays.asList(
        KV.of("amy", "111-222-3333"),
        KV.of("james", "222-333-4444"),
        KV.of("amy", "333-444-5555"),
        KV.of("carl", "444-555-6666"));

PCollection<KV<String, String>> emails = p.apply("CreateEmails", Create.of(emailsList));
PCollection<KV<String, String>> phones = p.apply("CreatePhones", Create.of(phonesList));
```

After ```CoGroupByKey```, the resulting data contains all data associated with each unique key from any of the input collections.

```
final TupleTag<String> emailsTag = new TupleTag<>();
final TupleTag<String> phonesTag = new TupleTag<>();

final List<KV<String, CoGbkResult>> expectedResults =
    Arrays.asList(
        KV.of(
            "amy",
            CoGbkResult.of(emailsTag, Arrays.asList("amy@example.com"))
                .and(phonesTag, Arrays.asList("111-222-3333", "333-444-5555"))),
        KV.of(
            "carl",
            CoGbkResult.of(emailsTag, Arrays.asList("carl@email.com", "carl@example.com"))
                .and(phonesTag, Arrays.asList("444-555-6666"))),
        KV.of(
            "james",
            CoGbkResult.of(emailsTag, Arrays.asList())
                .and(phonesTag, Arrays.asList("222-333-4444"))),
        KV.of(
            "julia",
            CoGbkResult.of(emailsTag, Arrays.asList("julia@example.com"))
                .and(phonesTag, Arrays.asList())));
```

The following code example joins the two `PCollection`s with `CoGroupByKey`, followed by a `ParDo` to consume the result. Then, the code uses tags to look up and format data from each collection.