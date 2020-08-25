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

# Side Input

In addition to the main input PCollection, you can provide additional inputs to a ParDo transform 
in the form of side inputs. A side input is an additional input that your DoFn can access each time 
it processes an element in the input PCollection. When you specify a side input, you create a view 
of some other data that can be read from within the ParDo transform’s DoFn while processing each 
element.

Side inputs are useful if your ParDo needs to inject additional data when processing each element 
in the input PCollection, but the additional data needs to be determined at runtime (and not 
hard-coded). Such values might be determined by the input data, or depend on a different branch of 
your pipeline.

**Kata:** Please enrich each Person with the country based on the city he/she lives in.

<div class="hint">
    Use <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#hdr-Side_Inputs">
    beam.SideInput</a> to create a side input of the citiesToCountries PCollection
</div>

<div class="hint">
    Use <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#ParDo">
    beam.ParDo</a> with a DoFn that accepts a
    <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#hdr-Side_Inputs">
    beam.SideInput</a>.
</div>

<div class="hint">
    Refer to the Beam Programming Guide
    <a href="https://beam.apache.org/documentation/programming-guide/#side-inputs">"Side inputs"</a>
    section for more information.
</div>

<div class="hint">
    A ParDo that has a PCollection KV as side input expects a DoFn that looks like the following.    
    
```
func doFn(element X, kvIterator func(*K, *V) bool, emit func(Y)) {
    // element of type X comes from your PCollection input
    // from your KV side input:     
    var key K   // expect a key of type K 
    var value V // expect a value of type V 
    for kvIterator(&key, &value) {
        // do something with key and value
        // emit(some result)
    }
} 
``` 
</div>