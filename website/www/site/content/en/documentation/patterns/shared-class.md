---
title: "Cache data using a shared object"
---

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

# Cache data using a shared object

A cache is a software component that stores data so that future requests for that data can be served faster. To access a cache, you can use side inputs, stateful `DoFn`, and calls to an external service. The Python SDK provides another option in the shared module. This option can be more memory-efficient than side inputs, simpler than a stateful `DoFn`, and more performant than calling an external service, because it does not have to access an external service for every element or bundle of elements. For more details about strategies for caching data using Beam SDK, see the session [Strategies for caching data in Dataflow using Beam SDK](https://2022.beamsummit.org/sessions/strategies-for-caching-data-in-dataflow-using-beam-sdk/) from the 2022 Beam Summit.

The examples on this page demonstrate how to use the `Shared` class of the [`shared module`](https://beam.apache.org/releases/pydoc/current/apache_beam.utils.shared.html) to enrich elements in both bounded and unbounded `PCollection` objects. Two data sets are used in the samples: _order_ and _customer_. The order records include customer IDs that customer attributes are added to by mapping the customer records.

## Create a cache on a batch pipeline

In this example, the customer cache is loaded as a dictionary in the `setup` method of the `EnrichOrderFn`. The cache is used to add customer attributes to the order records. Because the Python dictionary doesn't support weak references and a `Shared` object encapsulates a weak reference to a singleton instance of the shared resource, create a wrapper class.

{{< highlight py >}}
# The wrapper class is needed for a dictionary, because it does not support weak references.
class WeakRefDict(dict):
    pass

class EnrichOrderFn(beam.DoFn):
    def __init__(self):
        self._customers = {}
        self._shared_handle = shared.Shared()

    def setup(self):
        # setup is a good place to initialize transient in-memory resources.
        self._customer_lookup = self._shared_handle.acquire(self.load_customers)

    def load_customers(self):
        self._customers = expensive_remote_call_to_load_customers()
        return WeakRefDict(self._customers)

    def process(self, element):
        attr = self._customer_lookup.get(element["customer_id"], {})
        yield {**element, **attr}
{{< /highlight >}}

## Create a cache and update it regularly on a streaming pipeline

Because the customer cache is assumed to change over time, you need to refresh it periodically. To reload the shared object, change the `tag` argument of the `acquire` method. In this example, the refresh is implemented in the `start_bundle` method, where it compares the current tag value to the value that is associated with the existing shared object. The `set_tag` method returns a tag value that is the same within the maximum seconds of staleness. Therefore, if a tag value is greater than the existing tag value, it triggers a refresh of the customer cache.

{{< highlight py >}}
# The wrapper class is needed for a dictionary, because it does not support weak references.
class WeakRefDict(dict):
    pass

class EnrichOrderFn(beam.DoFn):
    def __init__(self):
        self._max_stale_sec = 60
        self._customers = {}
        self._shared_handle = shared.Shared()

    def setup(self):
        # setup is a good place to initialize transient in-memory resources.
        self._customer_lookup = self._shared_handle.acquire(
            self.load_customers, self.set_tag()
        )

    def set_tag(self):
        # A single tag value is returned within a period, which is upper-limited by the max stale second.
        current_ts = datetime.now().timestamp()
        return current_ts - (current_ts % self._max_stale_sec)

    def load_customers(self):
        # Assign the tag value of the current period for comparison.
        self._customers = expensive_remote_call_to_load_customers(tag=self.set_tag())
        return WeakRefDict(self._customers)

    def start_bundle(self):
        # Update the shared object when the current tag value exceeds the existing value.
        if self.set_tag() > self._customers["tag"]:
            self._customer_lookup = self._shared_handle.acquire(
                self.load_customers, self.set_tag()
            )

    def process(self, element):
        attr = self._customer_lookup.get(element["customer_id"], {})
        yield {**element, **attr}
{{< /highlight >}}