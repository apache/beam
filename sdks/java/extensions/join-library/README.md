Join-library
============

Join-library provides inner join, outer left and right join functions to
Google Cloud Dataflow. The aim is to simplify the most common cases of join to a
simple function call.

The functions are generic so it supports join of any types supported by
Dataflow. Input to the join functions are PCollections of Key/Values. Both the
left and right PCollections need the same type for the key. All the join
functions return a Key/Value where Key is the join key and value is
a Key/Value where the key is the left value and right is the value.

In the cases of outer join, since null cannot be serialized the user have
to provide a value that represent null for that particular use case.

Example how to use join-library:

    PCollection<KV<String, String>> leftPcollection = ...
    PCollection<KV<String, Long>> rightPcollection = ...

    PCollection<KV<String, KV<String, Long>>> joinedPcollection =
      Join.innerJoin(leftPcollection, rightPcollection);

Join-library can be found on maven-central:

    <dependency>
      <groupId>org.linuxalert.dataflow</groupId>
      <artifactId>google-cloud-dataflow-java-contrib-joinlibrary</artifactId>
      <version>0.0.3</version>
    </dependency>

Questions or comments: `M.Runesson [at] gmail [dot] com`
