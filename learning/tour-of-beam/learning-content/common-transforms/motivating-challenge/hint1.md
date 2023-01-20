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

To solve this challenge, you may build a pipeline that consists of the following steps:
{{if (eq .Sdk "go")}}
1. Filter so that the price is more than 15 or less using the `filter.Include`.
2. Summarize each `PCollection` using `stats.Sum`.
3. Return `KV` using the `ParDo` key, which will be `above` or `below`.
{{end}}
{{if (eq .Sdk "java")}}
1. Filter so that the price is more than 15 or less using the `Filter.by`.
2. Summarize each `PCollection` using `Sum.doublesGlobally`.
3. Return `KV` using the `WithKeys` key, which will be `above` or `below`. Instead of a `wildcard(<?>)` write `<KV<String,Double>>` .And create a `setCoder(KvCoder.of(StringUtf8Coder.of(),DoubleCoder.of()))` so that the pipeline knows what types of objects it is working with.
{{end}}
{{if (eq .Sdk "python")}}
1. Filter so that the price is more than 15 or less using the `beam.Filter`.
2. Summarize each `PCollection` using `beam.CombineGlobally(sum)`.
3. Return `KV` using the `WithKeys` key, which will be `above` or `below`.
{{end}}
