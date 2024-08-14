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

A cache is a software component that stores data so that future requests for that data can be served faster. Side inputs, stateful `DoFn` and calling an external service can be used for accessing a cache. The Python SDK provide another option in the shared module, which can be more efficient than side inputs in terms of the amount of memory required and simpler than stateful `DoFn`.

The samples on this page show you how to use the Shared class of the [shared module](https://beam.apache.org/releases/pydoc/current/apache_beam.utils.shared.html) to enrich elements in both bounded and unbounded `PCollection`s. Two data sets are used in the samples - order and customer. The order records include customer IDs with which customer attributes are added by mapping the customer records.

## Create a cache on a batch pipeline

In this example, the customer cache is loaded as a dictionary in the `setup` method of the `EnrichOrderFn`, and it is used to add customer attributes to the order records. Note that a Shared object encapsulates a weak reference to a singleton instance of the shared resource. Therefore, we need to create a wrapper class since the Python dictionary does not support weak references.

{{< highlight py >}}
import random
import logging
import time
from datetime import datetime
from uuid import uuid4

import apache_beam as beam
from apache_beam.utils import shared


def gen_customers(version: int, num_cust: int = 1000):
    d = dict()
    for r in range(num_cust):
        d[r] = {"version": version}
    d["timestamp"] = datetime.now().timestamp()
    return d


def gen_orders(ts: float, num_ord: int = 5, num_cust: int = 1000):
    orders = [
        {
            "order_id": str(uuid4()),
            "customer_id": random.randrange(1, num_cust),
            "timestamp": ts,
        }
        for _ in range(num_ord)
    ]
    for o in orders:
        yield o


# wrapper class needed for a dictionary since it does not support weak references.
class WeakRefDict(dict):
    pass


class EnrichOrderFn(beam.DoFn):
    def __init__(self, shared_handle):
        self._version = 1
        self._customers = {}
        self._shared_handle = shared_handle

    def setup(self):
        # setup is a good place to initialize transient in-memory resources.
        self._customer_lookup = self._shared_handle.acquire(self.load_customers)

    def load_customers(self):
        time.sleep(2)
        self._customers = gen_customers(version=self._version)
        return WeakRefDict(self._customers)

    def process(self, element):
        attr = self._customer_lookup.get(element["customer_id"], {})
        yield {**element, **attr}


with beam.Pipeline() as p:
    shared_handle = shared.Shared()
    (
        p
        | beam.Create(gen_orders(ts=datetime.now().timestamp()))
        | beam.ParDo(EnrichOrderFn(shared_handle))
        | beam.Map(print)
    )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")
{{< /highlight >}}

## Create a cache and update it regularly on a streaming pipeline

As the batch pipeline example, the customer cache is initialized as a dictionary in the `setup` method. To update the cache, it is refreshed when its timestamp is earlier than the defined threshold value of 5 seconds. The refreshment is implemented in the `start_bundle` method. Once the cached is updated, the order records are enriched with updated customer attributes. Note that it is necessary to change the `tag` argument of the `acquire` method to reload the shared object when it gets refreshed. The current timestamp value (`current_ts`) is used in this example.

{{< highlight py >}}
import random
import logging
import time
from datetime import datetime
from uuid import uuid4

import apache_beam as beam
from apache_beam.utils import shared
from apache_beam.transforms.periodicsequence import PeriodicImpulse


def gen_customers(version: int, num_cust: int = 1000):
    d = dict()
    for r in range(num_cust):
        d[r] = {"version": version}
    d["timestamp"] = datetime.now().timestamp()
    return d


def gen_orders(e: float, num_ord: int = 5, num_cust: int = 1000):
    orders = [
        {
            "order_id": str(uuid4()),
            "customer_id": random.randrange(1, num_cust),
            "timestamp": e,
        }
        for _ in range(num_ord)
    ]
    for o in orders:
        yield o


# wrapper class needed for a dictionary since it does not support weak references.
class WeakRefDict(dict):
    pass


class EnrichOrderFn(beam.DoFn):
    def __init__(self, shared_handle):
        self._max_stale_sec = 5
        self._version = 1
        self._customers = {}
        self._shared_handle = shared_handle

    def setup(self):
        # setup is a good place to initialize transient in-memory resources.
        self._customer_lookup = self._shared_handle.acquire(
            self.load_customers, datetime.now().timestamp()
        )

    def load_customers(self):
        time.sleep(2)
        self._customers = gen_customers(version=self._version)
        return WeakRefDict(self._customers)

    def start_bundle(self):
        # update the lookup table when the time difference exceeds the threshold.
        current_ts = datetime.now().timestamp()
        ts_diff = current_ts - self._customers["timestamp"]
        if ts_diff > self._max_stale_sec:
            logging.info(f"refresh customer cache after {ts_diff} seconds...")
            self._version += 1
            self._customer_lookup = self._shared_handle.acquire(
                self.load_customers, current_ts
            )

    def process(self, element):
        attr = self._customer_lookup.get(element["customer_id"], {})
        yield {**element, **attr}


with beam.Pipeline() as p:
    shared_handle = shared.Shared()
    (
        p
        | PeriodicImpulse(fire_interval=2, apply_windowing=False)
        | beam.FlatMap(gen_orders)
        | beam.ParDo(EnrichOrderFn(shared_handle))
        | beam.Map(print)
    )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")
{{< /highlight >}}