You can solve the challenge with this way:
1. First you need to divide `PCollection` by `beam.FlatMap(lambda line : [line.split(':')])`.
2. It is necessary to divide by indexes the key will be the word the value of its counting, and it is necessary to remove the extra spaces `beam.Map(lambda arr : (arr[0],int(arr[1].strip())))`.
3. Combine by key the values should be combined `(score1 + score2)`, create class which extend Combine `beam.CombinePerKey(sum)`
