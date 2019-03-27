---
layout: section
title: "Beam SQL: Apache Calcite support"
section_menu: section-menu/sdks.html
permalink: /documentation/dsls/sql/calcite/
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

# Beam SQL: Calcite support
This page describes the Calcite SQL dialect supported by Beam SQL.

## SQL Query Syntax

<pre>
select:  
      SELECT   
{ * | projectItem [, projectItem ]* }  
      FROM tableExpression  
      [ WHERE booleanExpression ]  
      [ GROUP BY { groupItem [, groupItem ]* } ]  
      [ HAVING booleanExpression ]

selectWithoutFrom:  
      SELECT  
         { * | projectItem [, projectItem ]* }

statement:  
      setStatement  
  |   resetStatement  
  |   query  
  |   insert

setStatement: SET identifier '=' expression

resetStatement:  
      RESET identifier  
  |   RESET ALL

insert:  
      INSERT INTO tablePrimary  
      [ '(' column [, column ]* ')' ]  
      query

query:  
      values  
  |   WITH withItem [ , withItem ]* query  
  |   {  
          select  
      |   selectWithoutFrom  
      |   query UNION [ ALL | DISTINCT ] query  
      |   query EXCEPT [ ALL | DISTINCT ] query  
      |   query MINUS [ ALL | DISTINCT ] query  
      |   query INTERSECT [ ALL | DISTINCT ] query  
      }  

      [[ ORDER BY orderItem [, orderItem ]* ] LIMIT [ start, ] { count | ALL } ]  
      [ OFFSET start { ROW | ROWS } ]  
      [ FETCH { FIRST | NEXT } [ count ] { ROW | ROWS } ONLY ]

withItem:  
      name  
      [ '(' column [, column ]* ')' ]  
      AS '(' query ')'

orderItem:  
      expression [ ASC | DESC ] [ NULLS FIRST | NULLS LAST ]
      
projectItem:  
      expression [ [ AS ] columnAlias ]  
  |   tableAlias . *
  
groupItem:  
      expression  
  |   '(' expression [, expression ]* ')'

tableExpression:  
      tableReference [, tableReference ]*  
  |   tableExpression [ NATURAL ] [ ( LEFT | RIGHT | FULL ) [ OUTER ] ] JOIN tableExpression [ joinCondition ]
  
tableReference: tablePrimary [ [ AS ] alias

tablePrimary:  
      [ [ catalogName . ] schemaName . ] tableName  
      '(' TABLE [ [ catalogName . ] schemaName . ] tableName ')'  
  |   tablePrimary [ EXTEND ] '(' columnDecl [, columnDecl ]* ')'  
  |   [ LATERAL ] '(' query ')'  
  |   UNNEST '(' expression ')' [ WITH ORDINALITY ]  
  |   [ LATERAL ] TABLE '(' [ SPECIFIC ] functionName '(' expression [, expression ]* ')' ')'

joinCondition:  
      ON booleanExpression  

columnDecl:  
      column type [ NOT NULL ]

values:  
      VALUES expression [, expression ]*
</pre>
  
See also the [documentation for Calcite SQL's syntax](http://calcite.apache.org/docs/reference.html#grammar).


## Operators and functions
The following table describes the Calcite SQL operators and functions supported by Beam SQL.

<table class="table-bordered table-striped">
  <tr><th>Operators and functions</th><th>Beam SQL support status</th></tr>
<tr><td><a href="http://calcite.apache.org/docs/reference.html#operator-precedence">Operator precedence</a></td><td>Yes</td></tr>
<tr><td><a href="http://calcite.apache.org/docs/reference.html#comparison-operators">Comparison operators</a></td><td class="style1">Yes</td></tr>
<tr><td><a href="http://calcite.apache.org/docs/reference.html#logical-operators">Logical operators</a></td><td>Yes</td></tr>
<tr><td><a href="http://calcite.apache.org/docs/reference.html#arithmetic-operators-and-functions">Arithmetic operators and functions</a></td><td>Yes</td></tr>
<tr><td><a href="http://calcite.apache.org/docs/reference.html#character-string-operators-and-functions">Character string operators and functions</a></td><td>Yes</td></tr>
<tr><td><a href="http://calcite.apache.org/docs/reference.html#binary-string-operators-and-functions">Binary string operators and functions</a></td><td>No</td></tr>
<tr><td><a href="http://calcite.apache.org/docs/reference.html#datetime-functions">Date/time functions</a></td><td>Yes</td></tr>
<tr><td><a href="http://calcite.apache.org/docs/reference.html#system-functions">System functions</a></td><td>No</td></tr>
<tr><td><a href="http://calcite.apache.org/docs/reference.html#conditional-functions-and-operators">Conditional functions and operators</a></td><td>Yes</td></tr>
<tr><td><a href="http://calcite.apache.org/docs/reference.html#type-conversion">Type conversion</a></td><td>Yes</td></tr>
<tr><td><a href="http://calcite.apache.org/docs/reference.html#value-constructors">Value constructors</a></td><td>No, except array</td></tr>
<tr><td><a href="http://calcite.apache.org/docs/reference.html#collection-functions">Collection functions</a></td><td>No</td></tr>
<tr><td><a href="http://calcite.apache.org/docs/reference.html#period-predicates">Period predicates</a></td><td>No</td></tr>
<tr><td><a href="http://calcite.apache.org/docs/reference.html#jdbc-function-escape">JDBC function escape</a></td><td>No</td></tr>
<tr><td><a href="http://calcite.apache.org/docs/reference.html#aggregate-functions">Aggregate functions</a></td>
<td>Use Beam SQL <a href="https://beam.apache.org/documentation/dsls/sql/aggregate-functions/">aggregate functions</a></td></tr>
<tr><td><a href="http://calcite.apache.org/docs/reference.html#window-functions">Window functions</a></td><td>No</td></tr>
<tr><td><a href="http://calcite.apache.org/docs/reference.html#grouping-functions">Grouping functions</a></td><td>No</td></tr>
<tr><td><a href="http://calcite.apache.org/docs/reference.html#grouped-window-functions">Grouped window functions</a></td><td>Use Beam SQL <a href="https://beam.apache.org/documentation/dsls/sql/windowing-and-triggering/">windowing and triggering</a></td></tr>
<tr><td><a href="http://calcite.apache.org/docs/reference.html#grouped-auxiliary-functions">Grouped auxiliary functions</a></td><td>Yes, except SESSION_END</td></tr>
<tr><td><a href="http://calcite.apache.org/docs/reference.html#spatial-functions">Spatial functions</a></td><td>No</td></tr>
<tr><td><a href="http://calcite.apache.org/docs/reference.html#geometry-creation-functions-3d">Geometry creation functions (3D)</a></td><td>No</td></tr>
<tr><td><a href="http://calcite.apache.org/docs/reference.html#geometry-predicates">Geometry predicates</a></td><td>No</td></tr>
<tr><td><a href="http://calcite.apache.org/docs/reference.html#json-functions">JSON functions</a></td><td>No</td></tr>
<tr><td><a href="http://calcite.apache.org/docs/reference.html#user-defined-functions">User-defined functions</a></td>
<td>Use Beam SQL <a href="https://beam.apache.org/documentation/dsls/sql/user-defined-functions/">user-defined functions</a>. You cannot call functions with <a href="http://calcite.apache.org/docs/reference.html#calling-functions-with-named-and-optional-parameters">named and optional parameters</a>.</td></tr>
<tr><td><a href="http://calcite.apache.org/docs/reference.html#match_recognize">MATCH_RECOGNIZE</a></td><td>No</td></tr>
<tr><td><a href="http://calcite.apache.org/docs/reference.html#ddl-extensions">DDL Extensions</a></td><td>Use Beam SQL <a href="https://beam.apache.org/documentation/dsls/sql/create-external-table/">CREATE EXTERNAL TABLE</a></td></tr>
</table>
