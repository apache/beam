---
type: languages
title: "Beam ZetaSQL conversion rules"
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

# Beam ZetaSQL conversion rules

Conversion includes, but is not limited to, casting and coercion:

+ Casting is explicit conversion and uses the `CAST()` function.
+ Coercion is implicit conversion, which Beam SQL performs
  automatically under the conditions described below.


The table below summarizes all possible `CAST`s and coercions. "Coercion To" applies to all *expressions* of a given data type (e.g. a column).

{{< table >}}
<table>
<thead>
<tr>
<th>From Type</th>
<th>CAST to</th>
<th>Coercion To</th>
</tr>
</thead>
<tbody>


<tr>
<td>INT64</td>
<td><span>INT64</span><br /><span>FLOAT64</span><br /><span>STRING</span><br /></td>
<td><span>FLOAT64</span><br /></td>
</tr>

<tr>
<td>FLOAT64</td>
<td><span>FLOAT64</span><br /></td>
<td>&nbsp;</td>
</tr>


<tr>
<td>BOOL</td>
<td><span>BOOL</span><br /></td>
<td>&nbsp;</td>
</tr>


<tr>
<td>STRING</td>
<td><span>INT64</span><br /><span>STRING</span><br /><span>BYTES</span><br /><span>TIMESTAMP</span><br /></td>
<td>&nbsp;</td>
</tr>


<tr>
<td>BYTES</td>
<td><span>BYTES</span><br /><span>STRING</span><br /></td>
<td>&nbsp;</td>
</tr>

<tr>
<td>TIMESTAMP</td>
<td><span>STRING</span><br /><span>TIMESTAMP</span><br /></td>
<td>&nbsp;</td>
</tr>


<tr>
<td>ARRAY</td>
<td>ARRAY</td>
<td>&nbsp;</td>
</tr>



<tr>
<td>STRUCT</td>
<td>STRUCT</td>
<td>&nbsp;</td>
</tr>


</tbody>
</table>
{{< /table >}}

## Casting

Syntax:

```
CAST(expr AS typename)
```

Cast syntax is used in a query to indicate that the result type of an
expression should be converted to some other type.

Example:

```
CAST(x=1 AS STRING)
```

This results in `"true"` if `x` is `1`, `"false"` for any other non-`NULL`
value, and `NULL` if `x` is `NULL`.

Casts between supported types that do not successfully map from the original
value to the target domain produce runtime errors. For example, casting
BYTES to STRING where the
byte sequence is not valid UTF-8 results in a runtime error.



When casting an expression `x` of the following types, these rules apply:

{{< table >}}
<table>
<tr>
<th>From</th>
<th>To</th>
<th>Rule(s) when casting <code>x</code></th>
</tr>
<tr>
<td>INT64</td>
<td>FLOAT64</td>
<td>Returns a close but potentially not exact
FLOAT64
value.</td>
</tr>
<tr>
<td>FLOAT64</td>
<td>STRING</td>
<td>Returns an approximate string representation.<br />
</td>
</tr>
<tr>
<td>STRING</td>
<td>BYTES</td>
<td>STRINGs are cast to BYTES using UTF-8 encoding. For example, the STRING "&copy;",
when cast to BYTES, would become a 2-byte sequence with the hex values C2 and
A9.</td>
</tr>

<tr>
<td>BYTES</td>
<td>STRING</td>
<td>Returns <code>x</code> interpreted as a UTF-8 STRING.<br />
For example, the BYTES literal
<code>b'\xc2\xa9'</code>, when cast to STRING, is interpreted as UTF-8 and
becomes the unicode character "&copy;".<br />
An error occurs if <code>x</code> is not valid UTF-8.</td>
</tr>

<tr>
<td>ARRAY</td>
<td>ARRAY</td>
<td>Must be the exact same ARRAY type.</td>
</tr>

<tr>
<td>STRUCT</td>
<td>STRUCT</td>
<td>Allowed if the following conditions are met:<br />
<ol>
<li>The two STRUCTs have the same number of fields.</li>
<li>The original STRUCT field types can be explicitly cast to the corresponding
target STRUCT field types (as defined by field order, not field name).</li>
</ol>
</td>
</tr>

</table>
{{< /table >}}


## Coercion

Beam SQL coerces the result type of an expression to another type if
needed to match function signatures.  For example, if function func() is defined to take a single argument of type INT64  and an expression is used as an argument that has a result type of FLOAT64, then the result of the expression will be coerced to INT64 type before func() is computed.