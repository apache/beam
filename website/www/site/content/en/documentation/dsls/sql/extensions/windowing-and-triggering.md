---
type: languages
title: "Beam DSLs: SQL"
aliases: /documentation/dsls/sql/windowing-and-triggering/
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

# Beam SQL extensions: Windowing and triggering

You can use Beam's windowing semantics in two ways:

 - you can configure windowing on your input `PCollections` before passing them
   to a `BeamSql` transform
 - you can use windowing extensions in your windowing query, which will override
   the windowing of your input `PCollections`

Triggering can only be used by setting it on your input `PCollections`; there
are no SQL extensions for specifying triggering.

This section covers the use of SQL extensions to directly apply windowing.

Beam SQL supports windowing functions specified in `GROUP BY` clause.
`TIMESTAMP` field is required in this case. It is used as event timestamp for
rows. 

Supported windowing functions:
* `TUMBLE`, or fixed windows. Example of how define a fixed window with duration of 1 hour:
``` 
    SELECT f_int, COUNT(*) 
    FROM PCOLLECTION 
    GROUP BY 
      f_int,
      TUMBLE(f_timestamp, INTERVAL '1' HOUR)
```
* `HOP`, or sliding windows. Example of how to define a sliding windows for every 30 minutes with 1 hour duration:
```
    SELECT f_int, COUNT(*)
    FROM PCOLLECTION 
    GROUP BY 
      f_int, 
      HOP(f_timestamp, INTERVAL '30' MINUTE, INTERVAL '1' HOUR)
```
* `SESSION`, session windows. Example of how to define a session window with 5 minutes gap duration:
```
    SELECT f_int, COUNT(*) 
    FROM PCOLLECTION 
    GROUP BY 
      f_int, 
      SESSION(f_timestamp, INTERVAL '5' MINUTE)
```

**Note:** When no windowing function is specified in the query, then windowing strategy of the input `PCollections` is unchanged by the SQL query. If windowing function is specified in the query, then the windowing function of the `PCollection` is updated accordingly, but trigger stays unchanged.

