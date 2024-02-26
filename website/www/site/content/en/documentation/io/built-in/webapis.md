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

{{< language-switcher java py go >}}

The Beam SDKs include a built-in transform, called RequestResponseIO that can read from and write to Web APIs such as
REST or gRPC.

## Before you start

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

{{< paragraph class="language-py" >}}
To use RequestResponseIO, install the Beam SDK by running `pip install apache-beam`
{{< /paragraph >}}

{{< paragraph class="language-go" wrap="span" >}}
At this time the Go SDK implementation of RequestResponseIO is not available. See tracker issue:
https://github.com/apache/beam/issues/30423.
{{< /paragraph >}}

## Additional resources

{{< paragraph class="language-java" wrap="span" >}}
* [RequestResponseIO source code](https://github.com/apache/beam/tree/master/sdks/java/io/rrio)
* [RequestResponseIO Javadoc](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/RequestResponseIO.html)
{{< /paragraph >}}

{{< paragraph class="language-py" wrap="span" >}}
* [RequestResponseIO source code](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/io/requestresponse.py)
* [RequestResponseIO PyDoc](https://beam.apache.org/releases/pydoc/current/apache_beam.io.requestresponse.html)
{{< /paragraph >}}

{{< paragraph class="language-go" wrap="span" >}}
TODO: see https://github.com/apache/beam/issues/30423.
{{< /paragraph >}}

## RequestResponseIO basics

### Minimal code

The minimal code needed to read from or write to Web APIs is:

{{< paragraph class="language-java" wrap="span" >}}
1. [Caller](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/Caller.html) implementation.
2. Instantiate [RequestResponseIO](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/RequestResponseIO.html).
{{< /paragraph >}}

{{< paragraph class="language-py" wrap="span" >}}
1. [Caller](https://beam.apache.org/releases/pydoc/current/apache_beam.io.requestresponse.html#apache_beam.io.requestresponse.Caller) implementation.
2. Instantiate [RequestResponseIO](https://beam.apache.org/releases/pydoc/current/apache_beam.io.requestresponse.html#apache_beam.io.requestresponse.RequestResponseIO).
{{< /paragraph >}}

{{< paragraph class="language-go" wrap="span" >}}
TODO: see https://github.com/apache/beam/issues/30423.
{{< /paragraph >}}

#### Implementing the Caller

{{< paragraph class="language-java" >}}
[Caller](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/Caller.html) requires
only one method override.
{{< /paragraph >}}

{{< highlight java >}}
class MyCaller<MyRequest, MyResponse> implements Caller<MyRequest, MyResponse> {
    @Override
    public MyResponse call(MyRequest request) throws UserCodeExecutionException {
        // Do something with request and return the response.
    }
}
{{< /highlight >}}

{{< highlight py >}}
# Pending https://github.com/apache/beam/issues/30422
{{< /highlight >}}

{{< highlight go >}}
// Pending https://github.com/apache/beam/issues/30423
{{< /highlight >}}

#### Instantiate RequestResponseIO

{{< paragraph class="language-java" >}}
Using [RequestResponseIO](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/RequestResponseIO.html)
is as simple as shown below. It minimally requires two parameters: the `Caller` and the expected
[Coder](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/coders/Coder.html) of the response.
It returns a [Result](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/Result.html)
that bundles any failures and the `PCollection` of successful responses.
{{< /paragraph >}}

{{< highlight java >}}
Coder<MyResponse> responseCoder = ...
PCollection<MyRequest> requests = ...

Result<MyResponse> result = requests.apply(RequestResponseIO.of(new MyCaller(), responseCoder));

// Do something with the responses.
result.getResponses().apply( ... );

// Apply failures to a dead letter sink.
result.getFailures().apply( ... );

{{< /highlight >}}

{{< highlight py >}}
# Pending https://github.com/apache/beam/issues/30422
{{< /highlight >}}

{{< highlight go >}}
// Pending https://github.com/apache/beam/issues/30423
{{< /highlight >}}

{{< paragraph >}}
`RequestResponseIO` takes care of everything else needed to invoke the `Caller` for each request, handling retries and exponential backoff,
when needed. It doesn't care what you do inside your `Caller`, whether you make raw HTTP calls or use client code.
{{< /paragraph >}}

### Working with HTTP calls directly

The example below shows how to use a HTTP client and make raw HTTP calls to download images.

#### Define Caller

{{< paragraph class="language-java" >}}
We implement the `Caller` taking a custom `ImageRequest` and `ImageResponse`.
The [package](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/package-summary.html)
provides additional error types that extend
[UserCodeExecutionException](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/UserCodeExecutionException.html)
that you can throw to signal additional states when calling an API:
[UserCodeQuotaException](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/UserCodeQuotaException.html),
[UserCodeRemoteSystemException](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/UserCodeRemoteSystemException.html),
and [UserCodeTimeoutException](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/UserCodeTimeoutException.html),
{{< /paragraph >}}

{{< highlight java >}}
{{< code_sample "examples/java/webapis/src/main/java/org/apache/beam/examples/webapis/HttpImageClient.java" webapis_java_image_caller >}}
{{< /highlight >}}

{{< highlight py >}}
# Pending https://github.com/apache/beam/issues/30422
{{< /highlight >}}

{{< highlight go >}}
// Pending https://github.com/apache/beam/issues/30423
{{< /highlight >}}

#### Define request

`ImageRequest` is the custom request we provide the `HttpImageClient`, defined in the example above, to invoke the HTTP call
that acquires the image.
{{< paragraph class="language-java" wrap="span" >}}This example happens to use [Google AutoValue](https://github.com/google/auto/blob/main/value/userguide/index.md),
but you can use any custom `Serializable` Java class as you would in any Beam `PCollection`,
including inherent Java classes such as `String`, `Double`, etc.{{< /paragraph >}}

{{< highlight java >}}
{{< code_sample "examples/java/webapis/src/main/java/org/apache/beam/examples/webapis/ImageRequest.java" webapis_java_image_request >}}
{{< /highlight >}}

{{< highlight py >}}
# Pending https://github.com/apache/beam/issues/30422
{{< /highlight >}}

{{< highlight go >}}
// Pending https://github.com/apache/beam/issues/30423
{{< /highlight >}}

#### Define response

`ImageResponse` is the custom response we return from the `HttpImageClient`, defined in the example above, that contains the image data
as a result of calling the remote server with the image URL. {{< paragraph class="language-java" wrap="span" >}}Again,
this example happens to use [Google AutoValue](https://github.com/google/auto/blob/main/value/userguide/index.md),
but you can use any custom `Serializable` Java class as you would in any Beam `PCollection`
including inherent Java classes such as `String`, `Double`, etc.{{< /paragraph >}}

{{< highlight java >}}
{{< code_sample "examples/java/webapis/src/main/java/org/apache/beam/examples/webapis/ImageResponse.java" webapis_java_image_response >}}
{{< /highlight >}}

{{< highlight py >}}
# Pending https://github.com/apache/beam/issues/30422
{{< /highlight >}}

{{< highlight go >}}
// Pending https://github.com/apache/beam/issues/30423
{{< /highlight >}}

{{< paragraph class="language-java" >}}
`RequestResponseIO` needs the response's
[Coder](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/coders/Coder.html)
as its second required parameter.
{{< /paragraph >}}

{{< highlight java >}}
{{< code_sample "examples/java/webapis/src/main/java/org/apache/beam/examples/webapis/ImageResponseCoder.java" webapis_java_image_response_coder >}}
{{< /highlight >}}

#### Acquire image data from URLs

Below shows an example how to bring everything together in an end-to-end pipeline. From a list of image URLs,
the example builds the `PCollection` of `ImageRequest`s that is applied to an instantiated `RequestResponseIO`,
using the `HttpImageClient` `Caller` implementation.

{{< paragraph class="language-java" >}}
Any failures, accessible from the `Result`'s `getFailures` getter, is outputted to logs. Alternatively, one could
write failures to a database or filesystem. These failures are a `PCollection` of
[ApiIOError](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/ApiIOError.html).
{{< /paragraph >}}
The pipeline outputs a summary of the downloaded image, its mimetype and size. 

{{< highlight java >}}
{{< code_sample "examples/java/webapis/src/main/java/org/apache/beam/examples/webapis/UsingHttpClientExample.java" webapis_java_http_get >}}
{{< /highlight >}}

{{< highlight py >}}
# Pending https://github.com/apache/beam/issues/30422
{{< /highlight >}}

{{< highlight go >}}
// Pending https://github.com/apache/beam/issues/30423
{{< /highlight >}}

{{< paragraph class="language-java" >}}
The output of the pipeline is shown below, showing the image URL, its MIME type and size.
{{< /paragraph >}}

{{< highlight java >}}
KV{https://storage.googleapis.com/generativeai-downloads/images/factory.png, mimeType=image/png, size=23130}
KV{https://storage.googleapis.com/generativeai-downloads/images/scones.jpg, mimeType=image/jpeg, size=394671}
KV{https://storage.googleapis.com/generativeai-downloads/images/cake.jpg, mimeType=image/jpeg, size=253809}
KV{https://storage.googleapis.com/generativeai-downloads/images/chocolate.png, mimeType=image/png, size=29375}
KV{https://storage.googleapis.com/generativeai-downloads/images/croissant.jpg, mimeType=image/jpeg, size=207281}
KV{https://storage.googleapis.com/generativeai-downloads/images/dog_form.jpg, mimeType=image/jpeg, size=1121752}
{{< /highlight >}}

{{< highlight py >}}
# Pending https://github.com/apache/beam/issues/30422
{{< /highlight >}}

{{< highlight go >}}
// Pending https://github.com/apache/beam/issues/30423
{{< /highlight >}}

### Using API client code

There are some API services that provide client code that one can use within the `Caller` implementation. Below is
an example that adapts
[Vertex AI Gemini Java Client](https://cloud.google.com/vertex-ai/docs/generative-ai/start/quickstarts/quickstart-multimodal)
to work in a Beam pipeline.

#### Define Caller with SetupTeardown

{{< paragraph class="language-java" >}}
The `Caller` implementation needs to implement one more interface that tells `RequestResponseIO` how to handle its
setup and teardown. These are situations when certain client code needs to open and close network connections, for
example, as part of its usage. Additionally, you may encounter scenarios where the client code is not serializable.

The [SetupTeardown](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/SetupTeardown.html)
interface has only two methods.
{{< /paragraph >}}

{{< highlight java >}}
interface SetupTeardown {
    void setup() throws UserCodeExecutionException;
    void teardown() throws UserCodeExecutionException;
}
{{< /highlight >}}

{{< highlight py >}}
# Pending https://github.com/apache/beam/issues/30422
{{< /highlight >}}

{{< highlight go >}}
// Pending https://github.com/apache/beam/issues/30423
{{< /highlight >}}

Below is an implementation of both `Caller` and `SetupTeardown` to interact with the
[Gemini AI API](https://cloud.google.com/vertex-ai/docs/generative-ai/model-reference/gemini). It has a bit more
boilerplate than the simple HTTP example above.

{{< paragraph class="language-java" >}}
Key to this example is that `com.google.cloud.vertexai.VertexAI`
and `com.google.cloud.vertexai.generativeai.GenerativeModel` are not serializable and therefore need to be
instantiated with `transient`. You can ignore `@MonotonicNonNull` if you java project does not use the
[https://checkerframework.org/](https://checkerframework.org/).

During the `setup` method is where the `GeminiAIClient`, instantiates `VertexAI` and `GenerativeModel`, finally closing
`VertexAI` during `teardown`. Finally, its `call` method looks similar to the HTTP example above, where it takes a
request, uses it to invoke an API, and returns the response.
{{< /paragraph >}}

{{< highlight java >}}
{{< code_sample "examples/java/webapis/src/main/java/org/apache/beam/examples/webapis/GeminiAIClient.java" webapis_java_gemini_ai_client >}}
{{< /highlight >}}

{{< highlight py >}}
# Pending https://github.com/apache/beam/issues/30422
{{< /highlight >}}

{{< highlight go >}}
// Pending https://github.com/apache/beam/issues/30423
{{< /highlight >}}

#### Ask Gemini AI to identify the image

Now let's combine the previous example of acquiring an image to this Gemini AI client to ask it to identify the image.

Below is what we saw previously but encapsulated in a convenience method. It takes a `List` of urls, and returns
a `PCollection` of `ImageResponse`s containing the image data.

{{< highlight java >}}
{{< code_sample "examples/java/webapis/src/main/java/org/apache/beam/examples/webapis/Images.java" webapis_java_get_images >}}
{{< /highlight >}}

{{< highlight py >}}
# Pending https://github.com/apache/beam/issues/30422
{{< /highlight >}}

{{< highlight go >}}
// Pending https://github.com/apache/beam/issues/30423
{{< /highlight >}}

Next we convert the `ImageResponse`s into a `PCollection` of `GenerateContentRequest`s.

{{< highlight java >}}
{{< code_sample "examples/java/webapis/src/main/java/org/apache/beam/examples/webapis/GeminiAIExample.java" webapis_java_build_ai_requests >}}
{{< /highlight >}}

{{< highlight py >}}
# Pending https://github.com/apache/beam/issues/30422
{{< /highlight >}}

{{< highlight go >}}
// Pending https://github.com/apache/beam/issues/30423
{{< /highlight >}}

{{< paragraph class="language-java" >}}
Finally, we apply the `PCollection` of `GenerateContentRequest`s to `RequestResponseIO`, instantiated using the
`GeminiAIClient`, defined above. Notice instead of `RequestResponseIO.of`, we are using
`RequestResponseIO.ofCallerAndSetupTeardown`. The reason for the verbose method name is due to Java's generics. This
just tells the compiler that we are providing an implementation of both the `Caller` and `SetupTeardown` interfaces.
{{< /paragraph >}}

{{< highlight java >}}
{{< code_sample "examples/java/webapis/src/main/java/org/apache/beam/examples/webapis/GeminiAIExample.java" webapis_java_ask_ai >}}
{{< /highlight >}}

{{< highlight py >}}
# Pending https://github.com/apache/beam/issues/30422
{{< /highlight >}}

{{< highlight go >}}
// Pending https://github.com/apache/beam/issues/30423
{{< /highlight >}}

The full end-to-end pipeline is shown below.

{{< highlight java >}}
{{< code_sample "examples/java/webapis/src/main/java/org/apache/beam/examples/webapis/GeminiAIExample.java" webapis_java_identify_image >}}
{{< /highlight >}}

{{< highlight py >}}
# Pending https://github.com/apache/beam/issues/30422
{{< /highlight >}}

{{< highlight go >}}
// Pending https://github.com/apache/beam/issues/30423
{{< /highlight >}}

Below shows an abbreviated output of running the full pipeline, where we see the result of Gemini AI identifying the images.
{{< highlight java >}}
KV{https://storage.googleapis.com/generativeai-downloads/images/chocolate.png, candidates {
    content {
        role: "model"
        parts {
            text: " This is a picture of a chocolate bar."
    }
}

KV{https://storage.googleapis.com/generativeai-downloads/images/dog_form.jpg, candidates {
    content {
        role: "model"
        parts {
            text: " The picture is a dog walking application form. It has two sections, one for information
                    about the dog and one for information about the owner. The dog\'s name is Fido,
                    he is a Cavoodle, and he is black and tan. He is 3 years old and has a friendly
                    temperament. The owner\'s name is Mark, and his phone number is 0491570006. He would
                    like Fido to be walked once a week on Tuesdays and Thursdays in the morning."
        }
    }
}

KV{https://storage.googleapis.com/generativeai-downloads/images/croissant.jpg
    content {
        role: "model"
        parts {
            text: " The picture shows a basket of croissants. Croissants are a type of pastry that is made
                    from a yeast-based dough that is rolled and folded several times in the rising process.
                    The result is a light, flaky pastry that is often served with butter, jam, or chocolate.
                    Croissants are a popular breakfast food and can also be used as a dessert or snack."
        }
    }
}
{{< /highlight >}}

{{< highlight py >}}
# Pending https://github.com/apache/beam/issues/30422
{{< /highlight >}}

{{< highlight go >}}
// Pending https://github.com/apache/beam/issues/30423
{{< /highlight >}}
