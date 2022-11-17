<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# CombinePerKey

For a collection of maps with a shared key, you can combine collections of values associated with each key into one combined value using `Combine`.

Initial data:
```
  KV<cat, 1>
  KV<dog, 5>
  KV<dog, 6>
  KV<cat, 3>
  KV<tree, 2>
  ...
```

After conversion:
```
  KV<cat, 4>
  KV<dog, 11>
  KV<tree, 2>
```

```
static PCollection<KV<String, Integer>> combine(PCollection<KV<String, Integer>> input) {
        return input.apply(Combine.perKey(new SumCombineFn()));
    }

static class SumCombineFn extends Combine.BinaryCombineFn<Integer> {
        @Override
        public Integer apply(Integer left, Integer right) {
            return left + right;
        }
    }
```