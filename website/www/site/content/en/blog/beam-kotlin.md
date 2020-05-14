---
title:  "Apache Beam + Kotlin = ❤️"
date:   2019-04-25 00:00:01 -0800
categories:
  - blog
aliases:
  - /blog/2019/04/25/beam-kotlin.html
authors:
        - harshithdwivedi

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


Apache Beam samples are now available in Kotlin!

<!--more-->

<img src="/images/blog/kotlin.png" alt="Kotlin" height="320" width="800" >

If you are someone who's been working with Java in your professional career; there's a good chance that you've also heard of [Kotlin](https://kotlinlang.org/), which is an Open Sourced, statically typed language for JVM and is mostly being favoured by Android Developers due to the many myriad features which enable more concise and cleaner code than Java without sacrificing performance or safety.

It gives us an immense pleasure to announce that we are also taking a step ahead in the same direction and releasing the samples for the Beam SDK in Kotlin alongside Java!
 
 (Note : At the time of writing this post, only the WordCount samples have been added in Koltin with more samples underway)


## Code Snippets
Here are few brief snippets of code that show how the Kotlin Samples compare to Java 

### Java

{{< highlight java >}}
 String filename = String.format(
                    "%s-%s-of-%s%s",
                    filenamePrefixForWindow(intervalWindow),
                    shardNumber,
                    numShards,
                    outputFileHints.suggestedFilenameSuffix);
{{< /highlight >}}

### Kotlin

{{< highlight java >}}
 // String templating
 val filename = "$filenamePrefixForWindow(intervalWindow)-$shardNumber-of-$numShards${outputFileHints.suggestedFilenameSuffix)"  
{{< /highlight >}}

### Java 

{{< highlight java >}}
public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
    @Override
    public String apply(KV<String, Long> input) {
        return input.getKey() + ": " + input.getValue();
    }
}
{{< /highlight >}}

## Kotlin

{{< highlight java >}}
public class FormatAsTextFn : SimpleFunction<KV<String, Long>, String>() {
    override fun apply(input: KV<String, Long>) = "${input.key} : ${input.value}"  //Single line functions
}
{{< /highlight >}}

### Java

{{< highlight java >}}
if(tableRow != null){
    formatAndInsert(tableRow);
}
{{< /highlight >}}

### Kotlin

{{< highlight java >}}
tableRow?.let{
    formatAndInsert(it)  // No need for null checks
}
{{< /highlight >}}

### Java

{{< highlight java >}}
String tableName = "testTable";
{{< /highlight >}}

### Kotlin

{{< highlight java >}}
val tableName = "testTable"  // Type inferencing
{{< /highlight >}}

## Contributors Welcomed!

While we're still adding more samples and streamlining the current ones, we would love to have your feedback on the code snippets.
You can find them over here : [https://github.com/apache/beam/tree/master/examples/kotlin](https://github.com/apache/beam/tree/master/examples/kotlin)

If you are using Kotlin with Apache Beam already; we would very much appreciate if you went ahead and help us convert the existing samples from Java into Koltin.

Thank you, and we are looking forward to feedback from you!
