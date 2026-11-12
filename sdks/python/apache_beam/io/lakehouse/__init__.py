#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""``apache_beam.io.lakehouse`` provides zero-copy metadata translation
between lakehouse table formats.

This module provides utilities to synthesize Iceberg table metadata from
existing Delta Lake transaction logs without copying the underlying Parquet
data files. Both Delta Lake and Apache Iceberg use Parquet for data storage,
so Iceberg manifests can point directly at Delta's Parquet files.

Usage::

    from apache_beam.io.lakehouse import translate_delta_to_iceberg

    metadata_path = translate_delta_to_iceberg(
        delta_table_uri="gs://my-bucket/delta/orders/",
        iceberg_warehouse="gs://my-bucket/iceberg/wh/",
        table_identifier="analytics.orders"
    )
"""

from apache_beam.io.lakehouse.translate import translate_delta_to_iceberg

__all__ = ["translate_delta_to_iceberg"]
