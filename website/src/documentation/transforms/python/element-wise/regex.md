---
layout: section
title: "Regex"
permalink: /documentation/transforms/python/elementwise/regex/
section_menu: section-menu/documentation.html
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

# Regex

<script type="text/javascript">
localStorage.setItem('language', 'language-py')
</script>

<table>
  <td>
    <a class="button" target="_blank"
        href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.util.html#apache_beam.transforms.util.Regex">
      <img src="https://beam.apache.org/images/logos/sdks/python.png"
          width="20px" height="20px" alt="Pydoc" />
      Pydoc
    </a>
  </td>
</table>
<br>
Filters input string elements based on a regex. May also transform them based on the matching groups.

## Examples

In the following examples, we create a pipeline with a `PCollection` of text strings.
Then, we use the `Regex` transform to search, replace, and split through the text elements using
[regular expressions](https://docs.python.org/3/library/re.html).

You can use tools to help you create and test your regular expressions, such as
[regex101](https://regex101.com/).
Make sure to specify the Python flavor at the left side bar.

Lets look at the
[regular expression `(?P<icon>[^\s,]+), *(\w+), *(\w+)`](https://regex101.com/r/Z7hTTj/3)
for example.
It matches anything that is not a whitespace `\s` (`[ \t\n\r\f\v]`) or comma `,`
until a comma is found and stores that in the named group `icon`,
this can match even `utf-8` strings.
Then it matches any number of whitespaces, followed by at least one word character
`\w` (`[a-zA-Z0-9_]`), which is stored in the second group for the *name*.
It does the same with the third group for the *duration*.

> *Note:* To avoid unexpected string escaping in your regular expressions,
> it is recommended to use
> [raw strings](https://docs.python.org/3/reference/lexical_analysis.html?highlight=raw#string-and-bytes-literals)
> such as `r'raw-string'` instead of `'escaped-string'`.

### Example 1: Regex match

`Regex.matches` keeps only the elements that match the regular expression,
returning the matched group.
The argument `group` is set to `0` (the entire match) by default,
but can be set to a group number like `3`, or to a named group like `'icon'`.

`Regex.matches` starts to match the regular expression at the beginning of the string.
To match until the end of the string, add `'$'` at the end of the regular expression.

To start matching at any point instead of the beginning of the string, use
[`Regex.find(regex)`](#example-4-regex-find).

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex.py tag:regex_matches %}```

Output `PCollection` after `Regex.matches`:

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex_test.py tag:plants_matches %}```

<table>
  <td>
    <a class="button" target="_blank"
        href="https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex.py">
      <img src="https://www.tensorflow.org/images/GitHub-Mark-32px.png"
        width="20px" height="20px" alt="View on GitHub" />
      View on GitHub
    </a>
  </td>
</table>
<br>

### Example 2: Regex match with all groups

`Regex.all_matches` keeps only the elements that match the regular expression,
returning *all groups* as a list.
The groups are returned in the order encountered in the regular expression,
including `group 0` (the entire match) as the first group.

`Regex.all_matches` starts to match the regular expression at the beginning of the string.
To match until the end of the string, add `'$'` at the end of the regular expression.

To start matching at any point instead of the beginning of the string, use
[`Regex.find_all(regex, group=Regex.ALL, outputEmpty=False)`](#example-5-regex-find-all).

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex.py tag:regex_all_matches %}```

Output `PCollection` after `Regex.all_matches`:

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex_test.py tag:plants_all_matches %}```

<table>
  <td>
    <a class="button" target="_blank"
        href="https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex.py">
      <img src="https://www.tensorflow.org/images/GitHub-Mark-32px.png"
        width="20px" height="20px" alt="View on GitHub" />
      View on GitHub
    </a>
  </td>
</table>
<br>

### Example 3: Regex match into key-value pairs

`Regex.matches_kv` keeps only the elements that match the regular expression,
returning a key-value pair using the specified groups.
The argument `keyGroup` is set to a group number like `3`, or to a named group like `'icon'`.
The argument `valueGroup` is set to `0` (the entire match) by default,
but can be set to a group number like `3`, or to a named group like `'icon'`.

`Regex.matches_kv` starts to match the regular expression at the beginning of the string.
To match until the end of the string, add `'$'` at the end of the regular expression.

To start matching at any point instead of the beginning of the string, use
[`Regex.find_kv(regex, keyGroup)`](#example-6-regex-find-as-key-value-pairs).

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex.py tag:regex_matches_kv %}```

Output `PCollection` after `Regex.matches_kv`:

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex_test.py tag:plants_matches_kv %}```

<table>
  <td>
    <a class="button" target="_blank"
        href="https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex.py">
      <img src="https://www.tensorflow.org/images/GitHub-Mark-32px.png"
        width="20px" height="20px" alt="View on GitHub" />
      View on GitHub
    </a>
  </td>
</table>
<br>

### Example 4: Regex find

`Regex.find` keeps only the elements that match the regular expression,
returning the matched group.
The argument `group` is set to `0` (the entire match) by default,
but can be set to a group number like `3`, or to a named group like `'icon'`.

`Regex.find` matches the first occurrence of the regular expression in the string.
To start matching at the beginning, add `'^'` at the beginning of the regular expression.
To match until the end of the string, add `'$'` at the end of the regular expression.

If you need to match from the start only, consider using
[`Regex.matches(regex)`](#example-1-regex-match).

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex.py tag:regex_find %}```

Output `PCollection` after `Regex.find`:

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex_test.py tag:plants_matches %}```

<table>
  <td>
    <a class="button" target="_blank"
        href="https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex.py">
      <img src="https://www.tensorflow.org/images/GitHub-Mark-32px.png"
        width="20px" height="20px" alt="View on GitHub" />
      View on GitHub
    </a>
  </td>
</table>
<br>

### Example 5: Regex find all

`Regex.find_all` returns a list of all the matches of the regular expression,
returning the matched group.
The argument `group` is set to `0` by default, but can be set to a group number like `3`, to a named group like `'icon'`, or to `Regex.ALL` to return all groups.
The argument `outputEmpty` is set to `True` by default, but can be set to `False` to skip elements where no matches were found.

`Regex.find_all` matches the regular expression anywhere it is found in the string.
To start matching at the beginning, add `'^'` at the start of the regular expression.
To match until the end of the string, add `'$'` at the end of the regular expression.

If you need to match all groups from the start only, consider using
[`Regex.all_matches(regex)`](#example-2-regex-match-with-all-groups).

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex.py tag:regex_find_all %}```

Output `PCollection` after `Regex.find_all`:

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex_test.py tag:plants_find_all %}```

<table>
  <td>
    <a class="button" target="_blank"
        href="https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex.py">
      <img src="https://www.tensorflow.org/images/GitHub-Mark-32px.png"
        width="20px" height="20px" alt="View on GitHub" />
      View on GitHub
    </a>
  </td>
</table>
<br>

### Example 6: Regex find as key-value pairs

`Regex.find_kv` returns a list of all the matches of the regular expression,
returning a key-value pair using the specified groups.
The argument `keyGroup` is set to a group number like `3`, or to a named group like `'icon'`.
The argument `valueGroup` is set to `0` (the entire match) by default,
but can be set to a group number like `3`, or to a named group like `'icon'`.

`Regex.find_kv` matches the first occurrence of the regular expression in the string.
To start matching at the beginning, add `'^'` at the beginning of the regular expression.
To match until the end of the string, add `'$'` at the end of the regular expression.

If you need to match as key-value pairs from the start only, consider using
[`Regex.matches_kv(regex)`](#example-3-regex-match-into-key-value-pairs).

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex.py tag:regex_find_kv %}```

Output `PCollection` after `Regex.find_kv`:

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex_test.py tag:plants_find_kv %}```

<table>
  <td>
    <a class="button" target="_blank"
        href="https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex.py">
      <img src="https://www.tensorflow.org/images/GitHub-Mark-32px.png"
        width="20px" height="20px" alt="View on GitHub" />
      View on GitHub
    </a>
  </td>
</table>
<br>

### Example 7: Regex replace all

`Regex.replace_all` returns the string with all the occurrences of the regular expression replaced by another string.
You can also use
[backreferences](https://docs.python.org/3/library/re.html?highlight=backreference#re.sub)
on the `replacement`.

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex.py tag:regex_replace_all %}```

Output `PCollection` after `Regex.replace_all`:

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex_test.py tag:plants_replace_all %}```

<table>
  <td>
    <a class="button" target="_blank"
        href="https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex.py">
      <img src="https://www.tensorflow.org/images/GitHub-Mark-32px.png"
        width="20px" height="20px" alt="View on GitHub" />
      View on GitHub
    </a>
  </td>
</table>
<br>

### Example 8: Regex replace first

`Regex.replace_first` returns the string with the first occurrence of the regular expression replaced by another string.
You can also use
[backreferences](https://docs.python.org/3/library/re.html?highlight=backreference#re.sub)
on the `replacement`.

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex.py tag:regex_replace_first %}```

Output `PCollection` after `Regex.replace_first`:

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex_test.py tag:plants_replace_first %}```

<table>
  <td>
    <a class="button" target="_blank"
        href="https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex.py">
      <img src="https://www.tensorflow.org/images/GitHub-Mark-32px.png"
        width="20px" height="20px" alt="View on GitHub" />
      View on GitHub
    </a>
  </td>
</table>
<br>

### Example 9: Regex split

`Regex.split` returns the list of strings that were delimited by the specified regular expression.
The argument `outputEmpty` is set to `False` by default, but can be set to `True` to keep empty items in the output list.

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex.py tag:regex_split %}```

Output `PCollection` after `Regex.split`:

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex_test.py tag:plants_split %}```

<table>
  <td>
    <a class="button" target="_blank"
        href="https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/regex.py">
      <img src="https://www.tensorflow.org/images/GitHub-Mark-32px.png"
        width="20px" height="20px" alt="View on GitHub" />
      View on GitHub
    </a>
  </td>
</table>
<br>

## Related transforms

* [FlatMap]({{ site.baseurl }}/documentation/transforms/python/elementwise/flatmap) behaves the same as `Map`, but for
  each input it may produce zero or more outputs.
* [Map]({{ site.baseurl }}/documentation/transforms/python/elementwise/map) applies a simple 1-to-1 mapping function over each element in the collection

<table>
  <td>
    <a class="button" target="_blank"
        href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.util.html#apache_beam.transforms.util.Regex">
      <img src="https://beam.apache.org/images/logos/sdks/python.png"
          width="20px" height="20px" alt="Pydoc" />
      Pydoc
    </a>
  </td>
</table>
<br>
