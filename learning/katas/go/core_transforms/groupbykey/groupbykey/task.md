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

# GroupByKey

GroupByKey is a Beam transform for processing collections of key/value pairs. It’s a parallel
reduction operation, analogous to the Shuffle phase of a Map/Shuffle/Reduce-style algorithm. The
input to GroupByKey is a collection of key/value pairs that represents a multimap, where the
collection contains multiple pairs that have the same key, but different values. Given such a
collection, you use GroupByKey to collect all of the values associated with each unique key.

**Kata:** Implement a 
<a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#GroupByKey">
beam.GroupByKey</a> transform that groups words by its first letter.

<div class="hint">
  Refer to <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#GroupByKey">
  beam.GroupByKey</a> to solve this problem.
</div>

<div class="hint">
  Refer to the Beam Programming Guide
  <a href="https://beam.apache.org/documentation/programming-guide/#groupbykey">
    "GroupByKey"</a> section for more information.
</div>

<div class="hint">
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
