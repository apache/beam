### Common Transforms motivating challenge

You need to group the words so that the key is the first letter and the value is an array of words. This can be achieved with `CombineFn`.

> **Importantly**. Since Go `DirectRunner` cannot parallelize, `MergeAccumulators` cannot be executed. Therefore, its logic will be contained in `AddInput`.