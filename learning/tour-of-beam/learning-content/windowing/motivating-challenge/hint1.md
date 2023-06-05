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
1. Add windowing `slidingWindowedItems := beam.WindowInto(s, window.NewSlidingWindows(5*time.Second, 10*time.Second), input)`
2. `max := getMax(s, slidingWindowedItems)`
3. `func getMax(s beam.Scope, input beam.PCollection) beam.PCollection {
   return stats.Max(s, input)
   }`
{{end}}

{{if (eq .Sdk "java")}}
1. Add sliding windowing `rideTotalAmounts.apply(
   Window.<Double>into(SlidingWindows.of(Duration.standardSeconds(10)).every(Duration.standardSeconds(5))))`
2. Write SerializableFunction which define max value: `.apply(Combine.globally((SerializableFunction<Iterable<Double>, Double>) input -> {
   Iterator<Double> iterator = input.iterator();
   double firstValue = iterator.hasNext() ? iterator.next() : : -Double.MAX_VALUE;
   for (double i : input){
   if(firstValue<i){
   firstValue = i;
   }
   }
   return firstValue;
   }).withoutDefaults());`

{{if (eq .Sdk "python")}}
1. Add window `'window' >>  beam.WindowInto(window.SlidingWindows(10, 5))`
2. Define max value:`'Sum above cost' >> beam.CombineGlobally(max).without_defaults()`
