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

The Beam SDKs include a built-in transform, called RequestResponseIO to support reads and writes with Web APIs such as
REST or gRPC.

Discussion below focuses on the Java SDK. Python examples will be added in the future; see tracker issue:
[#30422](https://github.com/apache/beam/issues/30422). Additionally, support for the Go SDK is not yet available;
see tracker issue: [#30423](https://github.com/apache/beam/issues/30423).


## RequestResponseIO Features

Features this transform provides include:
* developers provide minimal code that invokes Web API endpoint
* delegate to the transform to handle request retries and exponential backoff
* optional caching of request and response associations
* optional metrics

This guide currently focuses on the first two bullet points above, the minimal code requirements and error handling.
In the future, it may be expanded to show examples of additional features. Links to additional resources is
provided below.

## Additional resources

{{< paragraph class="language-java" wrap="span" >}}
* [RequestResponseIO source code](https://github.com/apache/beam/tree/master/sdks/java/io/rrio)
* [RequestResponseIO Javadoc](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/RequestResponseIO.html)
  {{< /paragraph >}}

## Before you start

{{< paragraph class="language-java" >}}
To use RequestResponseIO, add the dependency to your [Gradle](https://gradle.org) `build.gradle(.kts)` or
[Maven](https://maven.apache.org/) `pom.xml` file. See
[Maven Central](https://central.sonatype.com/artifact/org.apache.beam/beam-sdks-java-io-rrio) for available versions.
{{< /paragraph >}}

{{< paragraph class="language-java" >}}
Below shows an example adding the [Beam BOM](https://central.sonatype.com/artifact/org.apache.beam/beam-sdks-java-bom)
and related dependencies such as Beam core to your `build.gradle(.kts)` file.
{{< /paragraph >}}

{{< highlight java >}}
// Apache Beam BOM
// https://central.sonatype.com/artifact/org.apache.beam/beam-sdks-java-bom
implementation("org.apache.beam:beam-sdks-java-bom:{{< param release_latest >}}")

// Beam Core SDK
// https://central.sonatype.com/artifact/org.apache.beam/beam-sdks-java-core
implementation("org.apache.beam:beam-sdks-java-core")

// RequestResponseIO dependency
// https://central.sonatype.com/artifact/org.apache.beam/beam-sdks-java-io-rrio
implementation("org.apache.beam:beam-sdks-java-io-rrio")
{{< /highlight >}}

{{< paragraph class="language-java" >}}
Or using Maven, add the artifact dependency to your `pom.xml` file.
{{< /paragraph >}}

{{< highlight java >}}
<dependency>
    <groupId>org.apache.beam</groupId>
    <artifactId>beam-sdks-java-io-rrio</artifactId>
    <version>{{< param release_latest >}}</version>
</dependency>
{{< /highlight >}}


## RequestResponseIO basics

### Minimal code

The minimal code needed to read from or write to Web APIs is:

{{< paragraph class="language-java" wrap="span" >}}
1. [Caller](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/Caller.html) implementation.
2. Instantiate [RequestResponseIO](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/RequestResponseIO.html).
{{< /paragraph >}}

#### Implementing the Caller

{{< paragraph class="language-java" >}}
[Caller](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/Caller.html) requires
only one method override: [call](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/Caller.html#call-RequestT-), whose
purpose is to interact with the API, converting a request into a response.
The transform's DoFn invokes this method within its
[DoFn.ProcessElement](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/DoFn.ProcessElement.html)
method. The transform handles everything else including repeating failed requests and exponential backoff
(discussed more below).
{{< /paragraph >}}

{{< highlight java >}}
// MyCaller invokes a Web API with MyRequest and returns the resulting MyResponse.
class MyCaller<MyRequest, MyResponse> implements Caller<MyRequest, MyResponse> {

    @Override
    public MyResponse call(MyRequest request) throws UserCodeExecutionException {

        // Do something with request and return the response.

    }

}
{{< /highlight >}}

#### Instantiate RequestResponseIO

{{< paragraph class="language-java" >}}
Using [RequestResponseIO](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/RequestResponseIO.html)
is as simple as shown below. As mentioned, it minimally requires two parameters: the `Caller` and the expected
[Coder](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/coders/Coder.html) of the response. (_Note: If the concept of a Beam Coder is new to you, please see the
[Apache Beam Programming Guide](/documentation/programming-guide/#data-encoding-and-type-safety)
on this subject. This guide also has an example below._)
{{< /paragraph >}}

{{< paragraph class="language-java" >}}
The `RequestResponseIO` transform returns a [Result](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/Result.html)
that bundles any failures and the `PCollection` of successful responses. In Beam, we call this the
[additional outputs](/documentation/programming-guide/#additional-outputs) pattern,
which typically requires a bit of boilerplate but the transform takes care of it for you. Using the transform,
you get the success and failure `PCollection`s via
[Result::getFailures](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/Result.html#getFailures--)
and [Result::getResponses](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/Result.html#getResponses--).

Below shows an abbreviated snippet how the transform may work in your pipeline.
{{< /paragraph >}}

{{< highlight java >}}
// Step 1. Define the Coder for the response.
Coder<MyResponse> responseCoder = ...

// Step 2. Build the request PCollection.
PCollection<MyRequest> requests = ...

// Step 3. Instantiate the RequestResponseIO with the Caller and Coder and apply it to the request PCollection.
Result<MyResponse> result = requests.apply(RequestResponseIO.of(new MyCaller(), responseCoder));

// Step 4a. Do something with the responses.
result.getResponses().apply( ... );

// Step 4b. Apply failures to a dead letter sink.
result.getFailures().apply( ... );

{{< /highlight >}}

{{< paragraph >}}
`RequestResponseIO` takes care of everything else needed to invoke the `Caller` for each request. It doesn't care what
you do inside your `Caller`, whether you make raw HTTP calls or use client code. Later this guide discusses the
advantage of this design for testing.
{{< /paragraph >}}

### API call repeats and failures

As mentioned above, `RequestResponseIO` returns a
[Result](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/Result.html)
that bundles both the success and failure `PCollection`s resulting from your `Caller`. This section provides
a little more detail about handling failures and specifics on API call repeats with backoff.

#### Handling failures

The failures are an
[ApiIOError](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/ApiIOError.html)
`PCollection` that you may apply to a logging transform or a transform that
saves the errors to a downstream sink for later analysis and troubleshooting.

Since `ApiIOError` is already mapped to a Beam Schema, it has compatibility with most of Beam's existing I/O
connectors.
(_Note: If the concept of Beam Schemas is new to you, please see the
[Beam Programming Guide](/documentation/programming-guide/#schemas)._)
For example, you can easily send `ApiIOError` records to BigQuery for analysis and troubleshooting as shown
below **without** converting the records first to a
[TableRow](https://www.javadoc.io/doc/com.google.apis/google-api-services-bigquery/v2-rev20230812-2.0.0/com/google/api/services/bigquery/model/TableRow.html).

{{< highlight java >}}
{{< code_sample "examples/java/webapis/src/main/java/org/apache/beam/examples/webapis/AdditionalSnippets.java" webapis_java_write_failures_bigquery >}}
{{< /highlight >}}

#### API call repeats and backoff

Prior to emitting to the failure `PCollection`, the transform performs a retry **for certain errors**
after a prescribed exponential backoff. Your `Caller` must throw specific errors, to signal the transform
to perform the retry with backoff. Throwing a
[UserCodeExecutionException](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/UserCodeExecutionException.html)
will immediately emit the error into the `ApiIOError` `PCollection`.

`RequestResponseIO` will attempt a retry with backoff when `Caller` throws:
* [UserCodeQuotaException](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/UserCodeQuotaException.html)
* [UserCodeRemoteSystemException](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/UserCodeRemoteSystemException.html)
* [UserCodeTimeoutException](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/UserCodeTimeoutException.html)

After a threshold number of retries, the error is emitted into the failure `PCollection`.

#### Testing

Since `RequestResponseIO` doesn't care what you do inside your `Caller` implementation, this makes some testing more convenient.
Instead of relying on direct calls to a real API within some tests, consequently depending on your external resource,
you simply implement a version of your `Caller`
returning responses or throwing exceptions, according to your test logic.
For example, if you want to test a downstream step in your pipeline for a specific response, say empty records, you
could easily do so via the following. For more information on testing your Beam Pipelines, see
the [Beam Programming Guide](/documentation/pipelines/test-your-pipeline/).

{{< highlight java >}}

@Test
void givenEmptyResponse_thenExpectSomething() {
    // Test expects PTransform underTest should do something as a result of empty records, for example.
    PTransform<Iterable<String>, ?> underTest = ...

    PCollection<String> requests = pipeline.apply(Create.of("aRequest"));
    IterableCoder<String> coder = IterableCoder.of(StringUtf8Coder.of());
    Result<Iterable<String>> result = requests.apply(RequestResponseIO.of(new MockEmptyIterableResponse()), coder);

    PAssert.that(result.getResponses().apply(underTest)).containsInAnyOrder(...)

    pipeline.run();
}

// MockEmptyIterableResponse simulates when there are no results from the API.
class MockEmptyIterableResponse<String, Iterable<String>> implements Caller<String, Iterable<String>> {
@Override
    public Iterable<String> call(String request) throws UserCodeExecutionException {
        return Collections.emptyList();
    }
}
{{< /highlight >}}

## Practical examples

Below shows two examples that we will bring together in an end-to-end Beam pipeline. The goal of this pipeline is to
download images and use
[Gemini on Vertex AI](https://cloud.google.com/vertex-ai/generative-ai/docs/start/quickstarts/quickstart-multimodal)
to recognize the image content.

Note that this example does not replace our current AI/ML solutions. Please see
[Get started with AI/ML pipelines](/documentation/ml/overview/)
for more details on using Beam with AI/ML.

### Working with HTTP calls directly

We first need to download images. To do so, we need to make HTTP calls to the image URL and emit their content
into a `PCollection` for use with the Gemini API. The value of this example on its own is that it demonstrates
how to use `RequestResponseIO` to make raw HTTP requests.

#### Define Caller

We implement the `Caller`, the `HttpImageClient`, that receives an `ImageRequest` and returns an `ImageResponse`.

_For demo purposes, the example uses a
[KV](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/values/KV.html)
to preserve the raw URL in the returned `ImageResponse` containing `KV`._

##### Abbreviated snippet

Below shows an abbreviated version of the `HttpImageClient` showing the important parts.

{{< highlight java >}}
class HttpImageClient implements Caller<KV<String, ImageRequest>, KV<String, ImageResponse>> {

    private static final HttpRequestFactory REQUEST_FACTORY =
        new NetHttpTransport().createRequestFactory();

    @Override
    public KV<String, ImageResponse> call(KV<String, ImageRequest> requestKV) throws UserCodeExecutionException {

        ImageRequest request = requestKV.getValue();
        GenericUrl url = new GenericUrl(request.getImageUrl());
        HttpRequest imageRequest = REQUEST_FACTORY.buildGetRequest(url);
        HttpResponse response = imageRequest.execute();

        return KV.of(
            requestKV.getKey(),
            ImageResponse
                .builder()
                // Build ImageResponse from HttpResponse
                .build()
        );
    }

}
{{< /highlight >}}

##### Full example

The full implementation is shown below illustrating throwing various exceptions based on the HTTP response code.

{{< highlight java >}}
{{< code_sample "examples/java/webapis/src/main/java/org/apache/beam/examples/webapis/HttpImageClient.java" webapis_java_image_caller >}}
{{< /highlight >}}

#### Define request

`ImageRequest` is the custom request we provide the `HttpImageClient`, defined in the example above, to invoke the HTTP call
that acquires the image.
{{< paragraph class="language-java" wrap="span" >}}This example happens to use [Google AutoValue](https://github.com/google/auto/blob/main/value/userguide/index.md),
but you can use any custom `Serializable` Java class as you would in any Beam `PCollection`,
including inherent Java classes such as `String`, `Double`, etc. For convenience, this example uses
`@DefaultSchema(AutoValueSchema.class)` allowing us to map our custom type to a
[Beam Schema](/documentation/programming-guide/#schemas) automatically based on its getters.
{{< /paragraph >}}

{{< highlight java >}}
{{< code_sample "examples/java/webapis/src/main/java/org/apache/beam/examples/webapis/ImageRequest.java" webapis_java_image_request >}}
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

#### Define response coder

{{< paragraph class="language-java" >}}
`RequestResponseIO` needs the response's
[Coder](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/coders/Coder.html)
as its second required parameter, shown in the example below. Please see the
[Beam Programming Guide](https://beam.apache.org/documentation/programming-guide/#data-encoding-and-type-safety)
for more information about Beam Coders.
{{< /paragraph >}}

{{< highlight java >}}
{{< code_sample "examples/java/webapis/src/main/java/org/apache/beam/examples/webapis/ImageResponseCoder.java" webapis_java_image_response_coder >}}
{{< /highlight >}}

#### Acquire image data from URLs

Below shows an example how to bring everything together in an end-to-end pipeline. From a list of image URLs,
the example builds the `PCollection` of `ImageRequest`s that is applied to an instantiated `RequestResponseIO`,
using the `HttpImageClient` `Caller` implementation.

{{< paragraph class="language-java" >}}
Any failures, accessible from the `Result`'s `getFailures` getter, are outputted to logs. As already discussed above,
one could write these failures to a database or filesystem.
{{< /paragraph >}}

{{< highlight java >}}
{{< code_sample "examples/java/webapis/src/main/java/org/apache/beam/examples/webapis/UsingHttpClientExample.java" webapis_java_http_get >}}
{{< /highlight >}}

The pipeline output, shown below, displays a summary of the downloaded image, its URL, mimetype and size.

{{< highlight >}}
KV{https://storage.googleapis.com/generativeai-downloads/images/factory.png, mimeType=image/png, size=23130}
KV{https://storage.googleapis.com/generativeai-downloads/images/scones.jpg, mimeType=image/jpeg, size=394671}
KV{https://storage.googleapis.com/generativeai-downloads/images/cake.jpg, mimeType=image/jpeg, size=253809}
KV{https://storage.googleapis.com/generativeai-downloads/images/chocolate.png, mimeType=image/png, size=29375}
KV{https://storage.googleapis.com/generativeai-downloads/images/croissant.jpg, mimeType=image/jpeg, size=207281}
KV{https://storage.googleapis.com/generativeai-downloads/images/dog_form.jpg, mimeType=image/jpeg, size=1121752}
{{< /highlight >}}

### Using API client code

The last example demonstrated invoking HTTP requests directly. However, there are some API services that provide
client code that one should use within the `Caller` implementation. Using client code within Beam presents
unique challenges, namely serialization. Additionally, some client code requires explicit handling in terms of
setup and teardown.

{{< paragraph class="language-java" >}}
`RequestResponseIO` can handle an additional interface called `SetupTeardown` for these scenarios.

The [SetupTeardown](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/SetupTeardown.html)
interface has only two methods, setup and teardown.
{{< /paragraph >}}

{{< highlight java >}}
interface SetupTeardown {
    void setup() throws UserCodeExecutionException;
    void teardown() throws UserCodeExecutionException;
}
{{< /highlight >}}

{{< paragraph class="language-java" >}}
The transform calls these setup and teardown methods within its DoFn's
[@Setup](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/DoFn.Setup.html)
and
[@Teardown](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/DoFn.Teardown.html),
methods respectively.
{{< /paragraph >}}

The transform also handles retries with backoff, likewise dependent on the thrown Exception, as discussed previously
in this guide.

#### Define Caller with SetupTeardown

Below is
an example that adapts
[Vertex AI Gemini Java Client](https://cloud.google.com/vertex-ai/docs/generative-ai/start/quickstarts/quickstart-multimodal)
to work in a Beam pipeline using `RequestResponseIO`, adding usage of the `SetupTeardown` interface,
in addition to the required `Caller`. It has a bit more boilerplate than the simple HTTP example above.

{{< paragraph class="language-java" >}}

##### Abbreviated snippet

An abbreviated snippet showing the important parts is shown below.

The `setup` method is where the `GeminiAIClient` instantiates `VertexAI` and `GenerativeModel`, finally closing
`VertexAI` during `teardown`. Finally, its `call` method looks similar to the HTTP example above, where it takes a
request, uses it to invoke an API, and returns the response.
{{< /paragraph >}}

{{< highlight java >}}
class GeminiAIClient implements
    Caller<KV<String, GenerateContentRequest>, KV<String, GenerateContentResponse>>,
    SetupTeardown {

    @Override
    public KV<String, GenerateContentResponse> call(KV<String, GenerateContentRequest> requestKV)
    throws UserCodeExecutionException {
        GenerateContentResponse response = client.generateContent(request.getContentsList());
        return KV.of(requestKV.getKey(), response);
    }

    @Override
    public void setup() throws UserCodeExecutionException {
        vertexAI = new VertexAI(getProjectId(), getLocation());
        client = new GenerativeModel(getModelName(), vertexAI);
    }

    @Override
    public void teardown() throws UserCodeExecutionException {
        vertexAI.close();
    }
}
{{< /highlight >}}

##### Full example

Below shows the full example.
Key to this example is that `com.google.cloud.vertexai.VertexAI`
and `com.google.cloud.vertexai.generativeai.GenerativeModel` are not serializable and therefore need to be
instantiated with `transient`. _You can ignore `@MonotonicNonNull` if your java project does not use the
[https://checkerframework.org/](https://checkerframework.org/)_.

{{< highlight java >}}
{{< code_sample "examples/java/webapis/src/main/java/org/apache/beam/examples/webapis/GeminiAIClient.java" webapis_java_gemini_ai_client >}}
{{< /highlight >}}

#### Ask Gemini AI to identify the image

Now let's combine the previous example of acquiring an image to this Gemini AI client to ask it to identify the image.

Below is what we saw previously but encapsulated in a convenience method. It takes a `List` of urls, and returns
a `PCollection` of `ImageResponse`s containing the image data.

{{< highlight java >}}
{{< code_sample "examples/java/webapis/src/main/java/org/apache/beam/examples/webapis/Images.java" webapis_java_get_images >}}
{{< /highlight >}}

Next we convert the `ImageResponse`s into a `PCollection` of `GenerateContentRequest`s.

{{< highlight java >}}
{{< code_sample "examples/java/webapis/src/main/java/org/apache/beam/examples/webapis/GeminiAIExample.java" webapis_java_build_ai_requests >}}
{{< /highlight >}}

{{< paragraph class="language-java" >}}
Finally, we apply the `PCollection` of `GenerateContentRequest`s to `RequestResponseIO`, instantiated using the
`GeminiAIClient`, defined above. Notice instead of `RequestResponseIO.of`, we are using
`RequestResponseIO.ofCallerAndSetupTeardown`. The `ofCallerAndSetupTeardown` method just tells the compiler that we are
providing an implementation of both the `Caller` and `SetupTeardown` interfaces.
{{< /paragraph >}}

{{< highlight java >}}
{{< code_sample "examples/java/webapis/src/main/java/org/apache/beam/examples/webapis/GeminiAIExample.java" webapis_java_ask_ai >}}
{{< /highlight >}}

The full end-to-end pipeline is shown below.

{{< highlight java >}}
{{< code_sample "examples/java/webapis/src/main/java/org/apache/beam/examples/webapis/GeminiAIExample.java" webapis_java_identify_image >}}
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
