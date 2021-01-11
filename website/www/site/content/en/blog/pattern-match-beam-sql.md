---
title:  "Pattern Matching with Beam SQL"
date:   2020-08-27 00:00:01 +0800
aliases:
  - /blog/2020/08/27/pattern-match-beam-sql.html
categories:
  - blog
authors:
  - Mark-Zeng
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

## Introduction

SQL is becoming increasingly powerful and useful in the field of data analysis. MATCH\_RECOGNIZE,
a new SQL component introduced in 2016, brings extra analytical functionality. This project,
as part of Google Summer of Code, aims to support basic MATCH\_RECOGNIZE functionality. A basic MATCH\_RECOGNIZE
query would be something like this:
{{< highlight sql >}}
SELECT T.aid, T.bid, T.cid
FROM MyTable
    MATCH_RECOGNIZE (
      PARTITION BY userid
      ORDER BY proctime
      MEASURES
        A.id AS aid,
        B.id AS bid,
        C.id AS cid
      PATTERN (A B C)
      DEFINE
        A AS name = 'a',
        B AS name = 'b',
        C AS name = 'c'
    ) AS T
{{< /highlight >}}

The above query finds out ordered sets of events that have names 'a', 'b' and 'c'. Apart from this basic usage of
MATCH\_RECOGNIZE, I supported a few of other crucial features such as quantifiers and row pattern navigation. I will spell out
the details in later sections.

## Approach & Discussion

The implementation is strongly based on BEAM core transforms. Specifically, one MATCH\_RECOGNIZE execution composes the
following series of transforms:

1. A `ParDo` transform and then a `GroupByKey` transform that build up the partitions (PARTITION BY).
2. A `ParDo` transform that sorts within each partition (ORDER BY).
3. A `ParDo` transform that applies pattern-match in each sorted partition.

A pattern-match operation was first done with the java regex library. That is, I first transform rows within a partition into
a string and then apply regex pattern-match routines. If a row satisfies a condition, then I output the corresponding pattern variable.
This is ok under the assumption that the pattern definitions are mutually exclusive. That is, a pattern definition like `A AS A.price > 0, B AS b.price < 0` is allowed while
a pattern definition like `A AS A.price > 0, B AS B.proctime > 0` might results in an incomplete match. For the latter case,
an event can satisfy the conditions A and B at the same time. Mutually exclusive conditions gives deterministic pattern-match:
each event can only belong to at most one pattern class.

As specified in the SQL 2016 document, MATCH\_RECOGNIZE defines a richer set of expression than regular expression. Specifically,
it introduces *Row Pattern Navigation Operations* such as `PREV` and `NEXT`. This is perhaps one of the most intriguing feature of
MATCH\_RECOGNIZE. A regex library would no longer suffice the need since the pattern definition could be back-referencing (`PREV`) or
forward-referencing (`NEXT`). So for the second version of implementation, we chose to use an NFA regex engine. An NFA brings more flexibility
in terms of non-determinism (see Chapter 6 of SQL 2016 Part 5 for a more thorough discussion). My proposed NFA is based on a paper of UMASS.

This is a working project. Many of the components are still not supported. I will list some unimplemented work in the section
of future work.

## Usages

For now, the components I supported are:

- PARTITION BY
- ORDER BY
- MEASURES
    1. LAST
    2. FIRST
- ONE ROW PER MATCH/ALL ROWS PER MATCH
- DEFINE
    1. Left side of the condition
        1. LAST
    2. Right side of the condition
        1. PREV
- Quantifier
    1. Kleene plus

The pattern definition evaluation is hard coded. To be more specific, it expects the column reference of the incoming row
to be on the left side of a comparator. Additionally, PREV function can only appear on the right side of the comparator.

With these limited tools, we could already write some slightly more complicated queries. Imagine we have the following
table:

| transTime | price |
| :---: | :---: |
| 1 | 3 |
| 2 | 2 |
| 3 | 1 |
| 4 | 5 |
| 5 | 6 |

This table reflects the price changes of a product with respect to the transaction time. We could write the following
query:

{{< highlight sql >}}
SELECT *
FROM MyTable
    MATCH_RECOGNIZE (
      ORDER BY transTime
      MEASURES
        LAST(A.price) AS beforePrice,
        FIRST(B.price) AS afterPrice
      PATTERN (A+ B+)
      DEFINE
        A AS price < PREV(A.price),
        B AS price > PREV(B.price)
    ) AS T
{{< /highlight >}}

This will find the local minimum price and the price after it. For the example dataset, the first 3 rows will be
mapped to A and the rest of the rows will be mapped to B. Thus, we will have (1, 5) as the result.

> Very important: For my NFA implementation, it slightly breaks the rule in the SQL standard. Since the buffered NFA
only stores an event to the buffer if the event is a match to some pattern class, There would be no way to get the
previous event back if the previous row is discarded. So the first row would always be a match (different from the standard)
if PREV is used.

## Progress

1. PRs
    1. [Support MATCH\_RECOGNIZE using regex library](https://github.com/apache/beam/pull/12232) (merged)
    2. [Support MATCH\_RECOGNIZE using NFA](https://github.com/apache/beam/pull/12532) (pending)
2. Commits
    1. partition by: [commit 064ada7](https://github.com/apache/beam/pull/12232/commits/064ada7257970bcb1d35530be1b88cb3830f242b)
    2. order by: [commit 9cd1a82](https://github.com/apache/beam/pull/12232/commits/9cd1a82bec7b2f7c44aacfbd72f5f775bb58b650)
    3. regex pattern match: [commit 8d6ffcc](https://github.com/apache/beam/pull/12232/commits/8d6ffcc213e30999fc495c119b68da4f62fad258)
    4. support quantifiers: [commit f529b87](https://github.com/apache/beam/pull/12232/commits/f529b876a2c2e43d012c71b3a83ebd55eb16f4ff)
    5. measures: [commit 8793574](https://github.com/apache/beam/pull/12232/commits/87935746647611aa139d664ebed10c8e638bb024)
    6. added NFA implementation: [commit fc731f2](https://github.com/apache/beam/pull/12532/commits/fc731f2b0699d11853e7b76da86456427d434a2a)
    7. implemented functions PREV and LAST: [commit 35323da](https://github.com/apache/beam/pull/12532/commits/fc731f2b0699d11853e7b76da86456427d434a2a)

## Future Work

- Support FINAL/RUNNING keywords.
- Support more quantifiers.
- Add optimization to the NFA.
- A better way to realize MATCH\_RECOGNIZE might be having a Complex Event Processing library at BEAM core (rather than using BEAM transforms).

<!-- Related Documents:
        - proposal
        - design doc
        - SQL 2016 standard
        - UMASS NFA^b paper
-->

## References

- [Project Proposal](https://drive.google.com/file/d/1ZuFZV4dCFVPZW_-RiqbU0w-vShaZh_jX/view?usp=sharing)
- [Design Documentation](https://s.apache.org/beam-sql-pattern-recognization)
- [SQL 2016 documentation Part 5](https://www.iso.org/standard/65143.html)
- [UMASS paper on NFA with shared buffer](https://dl.acm.org/doi/10.1145/1376616.1376634)
