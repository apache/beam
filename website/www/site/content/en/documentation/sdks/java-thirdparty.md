---
type: languages
title: "Beam 3rd Party Java Extensions"
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
# Apache Beam 3rd Party Java Extensions

These are some of the 3rd party Java libaries that may be useful for specific applications.

## Parsing HTTPD/NGINX access logs.

### Summary
The Apache HTTPD webserver creates logfiles that contain valuable information about the requests that have been done to
the webserver. The format of these log files is a configuration option in the Apache HTTPD server so parsing this
into useful data elements is normally very hard to do.

To solve this problem in an easy way a library was created that works in combination with Apache Beam
and is capable of doing this for both the Apache HTTPD and NGINX.

The basic idea is that the logformat specification is the schema used to create the line. 
This parser is simply initialized with this schema and the list of fields you want to extract.

### Project page
[https://github.com/nielsbasjes/logparser](https://github.com/nielsbasjes/logparser) 

### License
Apache License 2.0

### Download
    <dependency>
      <groupId>nl.basjes.parse.httpdlog</groupId>
      <artifactId>httpdlog-parser</artifactId>
      <version>5.0</version>
    </dependency>

### Code example

Assuming a WebEvent class that has a the setters setIP, setQueryImg and setQueryStringValues

    PCollection<WebEvent> filledWebEvents = input
      .apply("Extract Elements from logline",
        ParDo.of(new DoFn<String, WebEvent>() {
          private Parser<WebEvent> parser;
    
          @Setup
          public void setup() throws NoSuchMethodException {
            parser = new HttpdLoglineParser<>(WebEvent.class, 
                "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\" \"%{Cookie}i\"");
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

### Summary
Parse and analyze the useragent string and extract as many relevant attributes as possible.

### Project page
[https://github.com/nielsbasjes/yauaa](https://github.com/nielsbasjes/yauaa) 

### License
Apache License 2.0

### Download
    <dependency>
      <groupId>nl.basjes.parse.useragent</groupId>
      <artifactId>yauaa-beam</artifactId>
      <version>4.2</version>
    </dependency>

### Code example
    PCollection<WebEvent> filledWebEvents = input
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

