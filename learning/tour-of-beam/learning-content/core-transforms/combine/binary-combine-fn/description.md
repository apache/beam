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

# BinaryCombineFn

A `BinaryCombineFn` is a `CombineFn` in Apache Beam that takes two input elements and produces one output element. It is used in the `Combine` transformation to combine elements of a `PCollection` into a single output value.

Here is an example of using a `BinaryCombineFn` to sum the elements of a `PCollection`:

```
static class SumBigIntegerFn extends BinaryCombineFn<BigInteger> {
    @Override
    public BigInteger apply(BigInteger left, BigInteger right) {
      return left.add(right);
    }
}
```

### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.

You can use other types instead of `BigInteger`:

```
static class SumIntegerFn extends BinaryCombineFn<Integer> {
    @Override
    public Integer apply(Integer left, Integer right) {
      return left+right;
    }
}
```