---
type: languages
title: "Beam ZetaSQL function call rules"
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

# Beam ZetaSQL function call rules

The following rules apply to all functions unless explicitly indicated otherwise in the function description:

+ For functions that accept numeric types, if one operand is a floating point
  operand and the other operand is another numeric type, both operands are
  converted to FLOAT64 before the function is
  evaluated.
+ If an operand is `NULL`, the result is `NULL`, with the exception of the
  IS operator.

+ For functions that are time zone sensitive (as indicated in the function
  description), the default time zone, UTC, is used if a time
  zone is not specified.