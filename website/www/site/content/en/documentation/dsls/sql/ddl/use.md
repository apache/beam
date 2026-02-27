---
type: languages
title: "Beam SQL DDL: Use"
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

# USE statements

The **USE** statement sets the active catalog or database for the current session.
This simplifies queries by allowing you to reference tables directly (e.g., `orders`),
avoiding the need for fully qualified names (e.g., `prod_catalog.sales_db.orders`).

***Tip**: the [SHOW CURRENT](/show) statement helps verify what the current context is.*

## USE CATALOG
Switches the current session's active Catalog.

_**Note:** All subsequent DATABASE and TABLE commands will be executed under this Catalog, unless fully qualified._

```sql
USE CATALOG prod_iceberg;
```

## USE DATABASE
Switches the current session's active Database.

_**Note:** All subsequent TABLE commands will be executed under this Database, unless fully qualified._

```sql
USE DATABASE sales_data;
```

Switch to a database in a specified catalog (_**Note:** this also switches the default to that catalog_):
```sql
USE DATABASE other_catalog.sales_data;
```
