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
### Windowing

Windowing subdivides a `PCollection` according to the timestamps of its individual elements. Transforms that aggregate multiple elements, such as GroupByKey and Combine, work implicitly on a per-window basis — they process each PCollection as a succession of multiple, finite windows, though the entire collection itself may be of unbounded size.

Some Beam transforms, such as `GroupByKey` and `Combine`, group multiple elements by a common key. Ordinarily, that grouping operation groups all the elements that have the same key within the entire data set. With an unbounded data set, it is impossible to collect all the elements, since new elements are constantly being added and may be infinitely many (e.g. streaming data). If you are working with unbounded PCollections, windowing is especially useful.


Fixed time windows are useful for performing time-based aggregations, such as counting the number of elements that arrived during each hour of the day. It allows you to group elements of a data set into fixed-length, non-overlapping time intervals, which can be useful for a variety of use cases.
For example, imagine you have a stream of data that is recording the number of website visitors every second, and you want to know the total number of visitors for each hour of the day. Using fixed-time windows, you can group the data into hour-long windows and then perform a sum aggregation on each window to get the total number of visitors for each hour.

Additionally, a fixed time window can also be helpful when dealing with data that arrive out-of-order or when dealing with late data. By specifying a fixed window duration, you can ensure that all elements that belong to a particular window are processed together, regardless of when they arrived.

To summarize, fixed time windows help perform **time-based aggregations** or handle **out-of-order or late data**.


`Sliding time windows` are similar to fixed time windows, but they have the added ability to move or slide over the data stream, allowing them to overlap.

One of the primary use cases for sliding time windows is to compute running aggregates. For example, if you want to calculate a running average of the past 60 seconds’ worth of data updated every 30 seconds, you can use sliding time windows. You can do this by defining a window duration of 60 seconds and a sliding interval of 30 seconds. With this configuration, you will have windows that slide every 30 seconds, each covering a 60-second interval.

Another use case for sliding time windows is to perform anomaly detection. By computing the running aggregates over a sliding window, you can detect patterns that deviate significantly from the historical data.

Sliding time windows also allows to look at data in a more dynamic way. This is useful when you have a high-frequency data stream and you want to look at the most recent data.

In summary, Sliding time windows are useful for performing running aggregations, anomaly detection and looking at data in a more dynamic way.


`Session windows` are a type of windowing that groups data elements based on periods of inactivity or "gaps" in the data stream. They are useful when you want to group data elements that are related to a specific event or activity together.

One of the main use cases for session windows is to group together data elements that are related to a user's session on a website or application. By using session windows with a relatively short gap duration, you can ensure that all the events related to a user's session are grouped together. This allows you to compute session-level metrics, such as the number of pages viewed per session, the duration of a session, or the number of events per session.

Another use case for session windows is to group together data elements that are related to a specific device's usage. For example, if you are collecting sensor data, you can use session windows to group together data elements that are collected while the device is in use. This allows you to compute device-level metrics, such as the number of sensor readings per device, the duration of device usage, or the number of events per device.

In summary, session windows are useful for grouping data elements that are related to specific events or activities, such as user sessions or device usage. This allows you to compute event- or device-level metrics.


A `single global window` is a type of windowing that treats all data elements as belonging to the same window. This means that all elements in the data stream are processed together and no windowing is applied.

The main use case for a single global window is when you want to process all the data elements in your data stream as a whole, without breaking them up into smaller windows. This can be useful in situations where you don't need to compute window-level metrics, such as running averages or counts, but instead want to process the entire data stream as a single unit.

For example, if you are using a data pipeline to filter out invalid data elements and then store the remaining data in a database, you might use a single global window to process all the data elements together, without breaking them up into smaller windows.

Another use case is when your data streams are already time-stamped and you want to process events in the order they arrive, so you don't want to group them based on time windows.

In summary, a single global window is useful when you want to process all the data elements in your data stream as a whole, without breaking them up into smaller windows. It can be useful for situations where you don't need to compute window-level metrics, or for processing events in the order they arrive.