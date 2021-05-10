package org.apache.beam.sdk.extensions.sql.zetasql;

import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

// To run as a gradle task
// task publishAvroToPubSub(type: JavaExec) {
//     main = "org.apache.beam.sdk.extensions.sql.zetasql.PublishAvroToPubSub"
//     classpath = sourceSets.main.runtimeClasspath
// }
public class PublishAvroToPubSub {

  public static void main(String[] args) throws Exception {
    GoogleCredentials credentials = GoogleCredentials.fromStream(
        new FileInputStream(
            "/usr/local/google/home/robinyq/Downloads/Dataflow cloud-workflows-9b5863035032.json"))
        .createScoped(
            Arrays.asList(
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/devstorage.full_control",
                "https://www.googleapis.com/auth/userinfo.email",
                "https://www.googleapis.com/auth/datastore",
                "https://www.googleapis.com/auth/pubsub"));
    publishAvroRecordsExample("google.com:clouddfe", "robinyq-avro-bin", credentials);
  }

  public static void publishAvroRecordsExample(String projectId, String topicId, GoogleCredentials credentials)
      throws IOException, ExecutionException, InterruptedException {

    TopicName topicName = TopicName.of(projectId, topicId);

    // Instantiate an avro-tools-generated class defined in `us-states.avsc`.
    Avro a = Avro.newBuilder().setStringField("hello").setIntField(1).build();

    Publisher publisher = null;
    try {
      publisher = Publisher.newBuilder(topicName).setCredentialsProvider(() -> credentials).build();

      // Prepare to serialize the object to the output stream.
      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

      // Encode the object and write it to the output stream.
      Encoder encoder = EncoderFactory.get().directBinaryEncoder(byteStream, /*reuse=*/ null);
      encoder.writeString("hello");
      encoder.writeInt(12);
      encoder.flush();

      // Publish the encoded object as a Pub/Sub message.
      ByteString data = ByteString.copyFrom(byteStream.toByteArray());
      PubsubMessage message = PubsubMessage.newBuilder().setData(data).build();
      System.out.println("Publishing message: " + message);

      ApiFuture<String> future = publisher.publish(message);
      System.out.println("Published message ID: " + future.get());

    } finally {
      if (publisher != null) {
        publisher.shutdown();
        publisher.awaitTermination(1, TimeUnit.MINUTES);
      }
    }
  }
}
