---
layout: section
title: "Beam SQL: SELECT Statement"
section_menu: section-menu/sdks.html
permalink: /documentation/dsls/sql/statements/select/
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

# SELECT

The main functionality of Beam SQL is the `SELECT` statement. This is how you
query and join data. The operations supported are a subset of
[Apache Calcite SQL](http://calcite.apache.org/docs/reference.html#grammar).

Generally, the semantics of queries is standard. Please see the following
sections to learn about extensions for supporting Beam's unified
batch/streaming model:

 - [Joins]({{ site.baseurl}}/documentation/dsls/sql/joins)
 - [Windowing & Triggering]({{ site.baseurl}}/documentation/dsls/sql/windowing-and-triggering/)

Below is a curated grammar of the supported syntax in Beam SQL

```
query:
	{
          select
      |   query UNION [ ALL ] query
      |   query MINUS [ ALL ] query
      |   query INTERSECT [ ALL ] query
	}
    [ ORDER BY orderItem [, orderItem ]* LIMIT count [OFFSET offset] ]

orderItem:
      expression [ ASC | DESC ]

select:
      SELECT
          { * | projectItem [, projectItem ]* }
      FROM tableExpression
      [ WHERE booleanExpression ]
      [ GROUP BY { groupItem [, groupItem ]* } ]
      [ HAVING booleanExpression ]

projectItem:
      expression [ [ AS ] columnAlias ]
  |   tableAlias . *

tableExpression:
      tableReference [, tableReference ]*
  |   tableExpression [ ( LEFT | RIGHT ) [ OUTER ] ] JOIN tableExpression [ joinCondition ]

booleanExpression:
    expression [ IS NULL | IS NOT NULL ]
  | expression [ > | >= | = | < | <= | <> ] expression
  | booleanExpression [ AND | OR ] booleanExpression
  | NOT booleanExpression
  | '(' booleanExpression ')'

joinCondition:
      ON booleanExpression

tableReference:
      tableName [ [ AS ] alias ]

values:
      VALUES expression [, expression ]*

groupItem:
      expression
  |   '(' expression [, expression ]* ')'
  |   HOP '(' expression [, expression ]* ')'
  |   TUMBLE '(' expression [, expression ]* ')'
  |   SESSION '(' expression [, expression ]* ')'

```

