<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# CoGroupByKey

CoGroupByKey performs a relational join of two or more key/value PCollections that have the same 
key type.

**Kata:** Implement a [beam.CoGroupByKey](https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#CoGroupByKey) 
transform that join words by the first alphabetical letter, and then produces the string representation of the 
WordsAlphabet model.

<div class="hint">
    Refer to
    <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#CoGroupByKey">beam.CoGroupByKey</a>
    to solve this problem.
</div>

<div class="hint">
  Refer to the Beam Programming Guide
  <a href="https://beam.apache.org/documentation/programming-guide/#cogroupbykey">
    "CoGroupByKey"</a> section for more information.
</div>

<div class="hint">
  Think of this problem in three stages.  First, create key/value pairs of PCollections called KV
  for fruits and countries, pairing the first character with the word.  Next, apply CoGroupByKey to the KVs
  followed by a ParDo.
</div>

<div class="hint">
  In the last lesson we learned how to make key/value PCollections called KV.  Now we have 
  two to make from fruits and countries.
  
  To return as a KV, you can return two values from your DoFn. The first return value represents the Key, and 
  the second return value represents the Value.  An example is shown below.
  
```
func doFn(element string) (string, string) {
    key := string(element[0])
    value := element
    return key, value
}
``` 
</div>

<div class="hint">
  In the last lesson we learned that 
  <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#GroupByKey">
  beam.GroupByKey</a> takes a single KV.
  <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#CoGroupByKey">beam.CoGroupByKey</a>
  takes more than one KV.
</div>

<div class="hint">
  Our final step in this problem requires a
  <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#ParDo">beam.ParDo</a>
  with a DoFn that's different than what we've seen in previous lessons.  In the previous step we should
  have a PCollection acquired from CoGroupByKey.  A ParDo for that PCollection expects a DoFn that looks
  like the following. 
  
  ```
  func doFn(key string, iterA func(*string) bool, iterB func(*string) bool, emit func(string)){
        ...
  }
  ```
  
  Each `func(*string) bool` parameter above corresponds to each of the PCollection KVs that we provided to CoGroupByKey.
  
  `func(*string)` returns true if there is a value available when the pipeline invokes your doFn.
  
  Note that it takes a `*string` instead of a `string`.  In Go, this means that to acquire its value, we need to
  do this.
  
```
    var v string
    iterA(&v)
    // do something with v
```

  Not necessary for this task, though if you expected multiple values per key, you would do something like this.
```
    var v string
    for iterA(&v) {
        // do something with v
    }
```
</div>
