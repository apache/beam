You can solve the challenge with this way:
1. First you need to divide `PCollection` by `FlatMapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
   .via((SerializableFunction<String, Iterable<KV<String, Integer>>>) line -> { ... }`.
2. Combine by key the values should be combined `(score1 + score2)`, create class which extend Combine `static class SumWordLetterCombineFn extends Combine.BinaryCombineFn<Integer>{}`
