# Sorter module

The provided Sorter can be used to sort large (more than can fit to memory)
amounts of data within the context of a Beam pipeline. It will default to in
memory sorting and spill to disk when the buffer is full.

ExternalSorter class implements the external sort algorithm (currently simply wrapping
the one from Hadoop). InMemorySorter implements the in memory sort and
BufferedExternalSorter uses fallback logic to combine the two.

SortValues is a PTransform that uses the BufferedExternalSorter to perform secondary key sorting.

Example of how to use sorter:

    PCollection<KV<String, KV<String, Integer>>> input = ...

    // Group by primary key, bringing <SecondaryKey, Value> pairs for the same key together.
    PCollection<KV<String, Iterable<KV<String, Integer>>>> grouped =
        input.apply(GroupByKey.<String, KV<String, Integer>>create());

    // For every primary key, sort the iterable of <SecondaryKey, Value> pairs by secondary key.
    PCollection<KV<String, Iterable<KV<String, Integer>>>> groupedAndSorted =
        grouped.apply(
            SortValues.<String, String, Integer>create(new BufferedExternalSorter.Options()));
