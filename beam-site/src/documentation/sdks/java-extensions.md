---
layout: section
title: "Beam Java SDK Extensions"
section_menu: section-menu/sdks.html
permalink: /documentation/sdks/java-extensions/
---
# Apache Beam Java SDK Extensions

## Join-library

Join-library provides inner join, outer left join, and outer right join functions. The aim
is to simplify the most common cases of join to a simple function call.

The functions are generic and support joins of any Beam-supported types.
Input to the join functions are `PCollections` of `Key` / `Value`s. Both
the left and right `PCollection`s need the same type for the key. All the join
functions return a `Key` / `Value` where `Key` is the join key and value is
a `Key` / `Value` where the key is the left value and right is the value.

For outer joins, the user must provide a value that represents `null` because `null`
cannot be serialized.

Example usage:

```
PCollection<KV<String, String>> leftPcollection = ...
PCollection<KV<String, Long>> rightPcollection = ...

PCollection<KV<String, KV<String, Long>>> joinedPcollection =
  Join.innerJoin(leftPcollection, rightPcollection);
```


## Sorter

This module provides the `SortValues` transform, which takes a `PCollection<KV<K, Iterable<KV<K2, V>>>>` and produces a `PCollection<KV<K, Iterable<KV<K2, V>>>>` where, for each primary key `K` the paired `Iterable<KV<K2, V>>` has been sorted by the byte encoding of secondary key (`K2`). It is an efficient and scalable sorter for iterables, even if they are large (do not fit in memory).

### Caveats

- This transform performs value-only sorting; the iterable accompanying each key is sorted, but *there is no relationship between different keys*, as Beam does not support any defined relationship between different elements in a `PCollection`.
* Each `Iterable<KV<K2, V>>` is sorted on a single worker using local memory and disk. This means that `SortValues` may be a performance and/or scalability bottleneck when used in different pipelines. For example, users are discouraged from using `SortValues` on a `PCollection` of a single element to globally sort a large `PCollection`. A (rough) estimate of the number of bytes of disk space utilized if sorting spills to disk is `numRecords * (numSecondaryKeyBytesPerRecord + numValueBytesPerRecord + 16) * 3`.

### Options

* The user can customize the temporary location used if sorting requires spilling to disk and the maximum amount of memory to use by creating a custom instance of `BufferedExternalSorter.Options` to pass into `SortValues.create`.

### Example usage of `SortValues`

```
PCollection<KV<String, KV<String, Integer>>> input = ...

// Group by primary key, bringing <SecondaryKey, Value> pairs for the same key together.
PCollection<KV<String, Iterable<KV<String, Integer>>>> grouped =
    input.apply(GroupByKey.<String, KV<String, Integer>>create());

// For every primary key, sort the iterable of <SecondaryKey, Value> pairs by secondary key.
PCollection<KV<String, Iterable<KV<String, Integer>>>> groupedAndSorted =
    grouped.apply(
        SortValues.<String, String, Integer>create(BufferedExternalSorter.options()));
```

## Parsing HTTPD/NGINX access logs.

The Apache HTTPD webserver creates logfiles that contain valuable information about the requests that have been done to
the webserver. The format of these config files is a configuration option in the Apache HTTPD server so parsing this
into useful data elements is normally very hard to do.

To solve this problem in an easy way a library was created that works in combination with Apache Beam
and is capable of doing this for both the Apache HTTPD and NGINX.

The basic idea is that the logformat specification is the schema used to create the line. 
THis parser is simply initialized with this schema and the list of fields you want to extract.

### Basic usage
Full documentation can be found here [https://github.com/nielsbasjes/logparser](https://github.com/nielsbasjes/logparser) 

First you put something like this in your pom.xml file:

    <dependency>
      <groupId>nl.basjes.parse.httpdlog</groupId>
      <artifactId>httpdlog-parser</artifactId>
      <version>5.0</version>
    </dependency>

Check [https://github.com/nielsbasjes/logparser](https://github.com/nielsbasjes/logparser) for the latest version.

Assume we have a logformat variable that looks something like this:

    String logformat = "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"";

**Step 1: What CAN we get from this line?**

To figure out what values we CAN get from this line we instantiate the parser with a dummy class
that does not have ANY @Field annotations or setters. The "Object" class will do just fine for this purpose.

    Parser<Object> dummyParser = new HttpdLoglineParser<Object>(Object.class, logformat);
    List<String> possiblePaths = dummyParser.getPossiblePaths();
    for (String path: possiblePaths) {
      System.out.println(path);
    }

You will get a list that looks something like this:

    IP:connection.client.host
    NUMBER:connection.client.logname
    STRING:connection.client.user
    TIME.STAMP:request.receive.time
    TIME.DAY:request.receive.time.day
    TIME.MONTHNAME:request.receive.time.monthname
    TIME.MONTH:request.receive.time.month
    TIME.YEAR:request.receive.time.year
    TIME.HOUR:request.receive.time.hour
    TIME.MINUTE:request.receive.time.minute
    TIME.SECOND:request.receive.time.second
    TIME.MILLISECOND:request.receive.time.millisecond
    TIME.ZONE:request.receive.time.timezone
    HTTP.FIRSTLINE:request.firstline
    HTTP.METHOD:request.firstline.method
    HTTP.URI:request.firstline.uri
    HTTP.QUERYSTRING:request.firstline.uri.query
    STRING:request.firstline.uri.query.*
    HTTP.PROTOCOL:request.firstline.protocol
    HTTP.PROTOCOL.VERSION:request.firstline.protocol.version
    STRING:request.status.last
    BYTESCLF:response.body.bytes
    HTTP.URI:request.referer
    HTTP.QUERYSTRING:request.referer.query
    STRING:request.referer.query.*
    HTTP.USERAGENT:request.user-agent

Now some of these lines contain a * .
This is a wildcard that can be replaced with any 'name' if you need a specific value.
You can also leave the '*' and get everything that is found in the actual log line.

**Step 2 Create the receiving POJO**

We need to create the receiving record class that is simply a POJO that does not need any interface or inheritance.
In this class we create setters that will be called when the specified field has been found in the line.

So we can now add to this class a setter that simply receives a single value as specified using the @Field annotation:

    @Field("IP:connection.client.host")
    public void setIP(final String value) {
      ip = value;
    }

If we really want the name of the field we can also do this

    @Field("STRING:request.firstline.uri.query.img")
    public void setQueryImg(final String name, final String value) {
      results.put(name, value);
    }

This latter form is very handy because this way we can obtain all values for a wildcard field

    @Field("STRING:request.firstline.uri.query.*")
    public void setQueryStringValues(final String name, final String value) {
      results.put(name, value);
    }

Instead of using the annotations on the setters we can also simply tell the parser the name of th setter that must be 
called when an element is found.

    parser.addParseTarget("setIP",                  "IP:connection.client.host");
    parser.addParseTarget("setQueryImg",            "STRING:request.firstline.uri.query.img");
    parser.addParseTarget("setQueryStringValues",   "STRING:request.firstline.uri.query.*");

### Example

Assuming we have a String (being the full log line) comming in and an instance of the WebEvent class comming out
(where the WebEvent already the has the needed setters) the final code when using this in an Apache Beam project 
will end up looking something like this

    PCollection<WebEvent> filledWebEvents = input
      .apply("Extract Elements from logline",
        ParDo.of(new DoFn<String, WebEvent>() {
          private Parser<WebEvent> parser;
    
          @Setup
          public void setup() throws NoSuchMethodException {
            parser = new HttpdLoglineParser<>(WebEvent.class, getLogFormat());
            parser.addParseTarget("setIP",                  "IP:connection.client.host");
            parser.addParseTarget("setQueryImg",            "STRING:request.firstline.uri.query.img");
            parser.addParseTarget("setQueryStringValues",   "STRING:request.firstline.uri.query.*");
          }
    
          @ProcessElement
          public void processElement(ProcessContext c) throws InvalidDissectorException, MissingDissectorsException, DissectionFailure {
            c.output(parser.parse(c.element()));
          }
        })
      );

## Analyzing the Useragent string

This is a java library that tries to parse and analyze the useragent string and extract as many relevant attributes as possible.

### Basic usage
You can get the prebuilt UDF from maven central.
If you use a maven based project simply add this dependency to your Apache Beam application.

    <dependency>
      <groupId>nl.basjes.parse.useragent</groupId>
      <artifactId>yauaa-beam</artifactId>
      <version>4.2</version>
    </dependency>

Check https://github.com/nielsbasjes/yauaa for the latest version.

### Example
Assume you have a PCollection with your records.
In most cases I see (clickstream data) these records (in this example this class is called "WebEvent") 
contain the useragent string in a field and the parsed results must be added to these fields.

Now you must do two things:

  1) Determine the names of the fields you need. Simply call getAllPossibleFieldNamesSorted() to get the list of possible fieldnames you can ask for.

    UserAgentAnalyzer.newBuilder().build()
      .getAllPossibleFieldNamesSorted()
        .forEach(field -> System.out.println(field));

and you get something like this:

    DeviceClass
    DeviceName
    DeviceBrand
    DeviceCpu
    DeviceCpuBits
    DeviceFirmwareVersion
    DeviceVersion
    OperatingSystemClass
    OperatingSystemName
    OperatingSystemVersion
    ...

  2) Add an instance of the (abstract) UserAgentAnalysisDoFn function and implement the functions as shown in the example below. Use the YauaaField annotation to get the setter for the requested fields.

Note that the name of the two setters is not important, the system looks at the annotation.

    .apply("Extract Elements from Useragent",
      ParDo.of(new UserAgentAnalysisDoFn<WebEvent>() {
        @Override
        public String getUserAgentString(WebEvent record) {
          return record.useragent;
        }

        @YauaaField("DeviceClass")
        public void setDC(WebEvent record, String value) {
          record.deviceClass = value;
        }

        @YauaaField("AgentNameVersion")
        public void setANV(WebEvent record, String value) {
          record.agentNameVersion = value;
        }
    }));

