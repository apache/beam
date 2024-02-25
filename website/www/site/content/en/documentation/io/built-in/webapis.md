---
title: "Web Apis I/O connector"
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

[Built-in I/O Transforms](/documentation/io/built-in/)

# Web APIs I/O connector

{{< language-switcher java >}}

The Beam SDKs include a built-in transform, called RequestResponseIO that can read from and write to Web APIs such as
REST or gRPC.

## Before you start

<!-- Java specific -->

{{< paragraph class="language-java" >}}
To use RequestResponseIO, add the Maven artifact dependency to your `pom.xml` file. See
[Maven Central](https://central.sonatype.com/artifact/org.apache.beam/beam-sdks-java-io-rrio) for available versions.
{{< /paragraph >}}

{{< highlight java >}}
<dependency>
    <groupId>org.apache.beam</groupId>
    <artifactId>beam-sdks-java-io-rrio</artifactId>
    <version>{{< param release_latest >}}</version>
</dependency>
{{< /highlight >}}

## Additional resources

{{< paragraph class="language-java" >}}
Additional resources:
{{< /paragraph >}}

{{< paragraph class="language-java" >}}
* [RequestResponseIO source code](https://github.com/apache/beam/tree/master/sdks/java/io/rrio)
* [RequestResponseIO Javadoc](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/RequestResponseIO.html)
{{< /paragraph >}}

## RequestResponseIO basics

### Minimal code

The minimal code needed to read from or write to Web APIs is:
1. [Caller](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/Caller.html) implementation.
2. Instantiate [RequestResponseIO](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/RequestResponseIO.html)

#### Implementing the Caller

[Caller](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/Caller.html) requires
only one method override.

{{< highlight java >}}
class MyCaller<MyRequest, MyResponse> implements Caller<MyRequest, MyResponse> {
    @Override
    public MyResponse call(MyRequest request) throws UserCodeExecutionException {
        // Do something with request and return the response.
    }
}
{{< /highlight >}}

#### Instantiate RequestResponseIO

Using [RequestResponseIO](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/RequestResponseIO.html)
is as simple as shown below. It returns a [Result](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/Result.html)
that bundles any failures and the `PCollection` of successful responses.

{{< highlight java >}}
Coder<MyResponse> responseCoder = ...
PCollection<MyRequest> requests = ...

Result<MyResponse> result = requests.apply(RequestResponseIO.of(new MyCaller(), responseCoder));

// Do something with the responses.
result.getResponses().apply( ... );

// Apply failures to a dead letter sink.
result.getFailures().apply( ... );

{{< /highlight >}}

[RequestResponseIO](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/RequestResponseIO.html)
takes care of everything else needed to invoke the `Caller` for each request, handling retries and exponential backoff,
when needed. It doesn't care what you do inside your `Caller`, whether you make raw HTTP calls or use client code.

### Working with HTTP calls directly

The example below shows how to use a HTTP client and make raw HTTP calls to download images.

#### Define Caller

We implement the `Caller` taking a custom `ImageRequest` and `ImageResponse`. 
The [package](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/package-summary.html)
provides additional error types that extend
[UserCodeExecutionException](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/UserCodeExecutionException.html)
that you can throw to signal additional states when calling an API:
[UserCodeQuotaException](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/UserCodeQuotaException.html),
[UserCodeRemoteSystemException](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/UserCodeRemoteSystemException.html),
and [UserCodeTimeoutException](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/UserCodeTimeoutException.html),

{{< highlight java >}}
{{< code_sample "examples/java/webapis/src/main/java/org/apache/beam/examples/webapis/HttpImageClient.java" webapis_image_caller >}}
{{< /highlight >}}

#### Define request

`ImageRequest` is the custom request we provide the `HttpImageClient`, defined in the example above, to invoke the HTTP call
that acquires the image.

{{< highlight java >}}
{{< code_sample "examples/java/webapis/src/main/java/org/apache/beam/examples/webapis/HttpImageClient.java" webapis_image_request >}}
{{< /highlight >}}

#### Define response

`ImageResponse` is the custom response we return from the `HttpImageClient`, defined in the example above, that contains the image data
as a result of calling the remote server with the image URL.

{{< highlight java >}}
{{< code_sample "examples/java/webapis/src/main/java/org/apache/beam/examples/webapis/ImageResponse.java" webapis_image_response >}}
{{< /highlight >}}

#### Acquire image data from URLs

Below shows an example how to bring everything together in an end-to-end pipeline. From a list of image URLs,
the example builds the `PCollection` of `ImageRequest`s that is applied to an instantiated `RequestResponseIO`,
using the `HttpImageClient` `Caller` implementation.

Any failures, accessible from the `Result`'s `getFailures` getter, is outputted to logs. Alternatively, one could
write failures to a database or filesystem. These failures are a `PCollection` of
[ApiIOError](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/ApiIOError.html).

Responses, retrieved from `Result`'s `getResponses` getter, gives us the `PCollection` of `ImageResponse`s, the
custom type defined above that holds the image data.

{{< highlight java >}}
{{< code_sample "examples/java/webapis/src/main/java/org/apache/beam/examples/webapis/UsingHttpClientExample.java" webapis_http_get >}}
{{< /highlight >}}

### Using API client code

There are some API services that provide client code that one can use within the `Caller` implementation. Below is
an example using the
[Vertex AI Gemini Java Client](https://cloud.google.com/vertex-ai/docs/generative-ai/start/quickstarts/quickstart-multimodal#gemini-beginner-samples-java).

#### Define Caller with SetupTeardown

The `Caller` implementation needs to implement one more interface that tells `RequestResponseIO` how to handle its
setup and teardown. These are situations when certain client code needs to open and close network connections, for
example, as part of its usage. Additionally, you may encounter scenarios where the client code is not serializable.

The [SetupTeardown](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/SetupTeardown.html)
interface has only two methods.

{{< highlight java >}}
interface SetupTeardown {
    void setup() throws UserCodeExecutionException;
    void teardown() throws UserCodeExecutionException;
}
{{< /highlight >}}

Below is an implementation of both `Caller` and `SetupTeardown` to interact with the
[Gemini AI API](https://cloud.google.com/vertex-ai/docs/generative-ai/model-reference/gemini). It has a bit more
boilerplate than the simple HTTP example above.

Key to this example is that `com.google.cloud.vertexai.VertexAI`
and `com.google.cloud.vertexai.generativeai.GenerativeModel` are not serializable and therefore need to be
instantiated with `transient`. You can ignore `@MonotonicNonNull` if you java project does not use the
[https://checkerframework.org/](https://checkerframework.org/).

During the `setup` method is where the `GeminiAIClient`, instantiates `VertexAI` and `GenerativeModel`, finally closing
`VertexAI` during `teardown`. Finally, its `call` method looks similar to the HTTP example above, where it takes a
request, uses it to invoke an API, and returns the response.

{{< highlight java >}}
{{< code_sample "examples/java/webapis/src/main/java/org/apache/beam/examples/webapis/GeminiAIClient.java" webapis_gemini_ai_client >}}
{{< /highlight >}}

#### Ask Gemini AI to identify the image

Now let's combine the previous example of acquiring an image to this Gemini AI client to ask it to identify the image.

Below is what we saw previously but encapsulated in a convenience method. It takes a `List` of urls, and returns
a `PCollection` of `ImageResponse`s containing the image data.

{{< highlight java >}}
{{< code_sample "examples/java/webapis/src/main/java/org/apache/beam/examples/webapis/Images.java" webapis_get_images >}}
{{< /highlight >}}

Next we convert the `ImageResponse`s into a `PCollection` of `GenerateContentRequest`s.

{{< highlight java >}}
{{< code_sample "examples/java/webapis/src/main/java/org/apache/beam/examples/webapis/GeminiAIExample.java" webapis_build_ai_requests >}}
{{< /highlight >}}

Finally, we apply the `PCollection` of `GenerateContentRequest`s to `RequestResponseIO`, instantiated using the
`GeminiAIClient`, defined above. Notice instead of `RequestResponseIO.of`, we are using
`RequestResponseIO.ofCallerAndSetupTeardown`. The reason for the verbose method name is due to Java's generics. This
just tells the compiler that we are providing an implementation of both the `Caller` and `SetupTeardown` interfaces.

{{< highlight java >}}
{{< code_sample "examples/java/webapis/src/main/java/org/apache/beam/examples/webapis/GeminiAIExample.java" webapis_ask_ai >}}
{{< /highlight >}}

The full end-to-end pipeline is shown below.

{{< highlight java >}}
{{< code_sample "examples/java/webapis/src/main/java/org/apache/beam/examples/webapis/GeminiAIExample.java" webapis_identify_image >}}
{{< /highlight >}}

#### An aside about Coders

You may run into issues with [Coder](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/coders/Coder.html)
and your own custom types. Below shows an example of how to define your own `Coder` for a custom type. The example
below may be helpful, since it also shows a situation where you don't own the custom type.

{{< highlight java >}}
{{< code_sample "examples/java/webapis/src/main/java/org/apache/beam/examples/webapis/GenerateContentRequestCoder.java" webapis_custom_coder >}}
{{< /highlight >}}
