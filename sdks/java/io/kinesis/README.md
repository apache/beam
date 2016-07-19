## Usage

Main class you're going to operate is called KinesisIO. It follows the usage conventions laid out by other *IO classes like BigQueryIO or PubsubIO

Let's see how you can set up a simple Pipeline, which reads from Kinesis:


    p.apply(KinesisIO.Read.
            from("streamName", InitialPositionInStream.LATEST).
            using("AWS_KEY", _"AWS_SECRET", STREAM_REGION).
    apply( ... ) // other transformations

As you can see you need to provide 3 things:

* name of the stream you're going to read
* position in the stream where reading should start. There are two options:
    * LATEST - reading will begin from end of the stream
    * TRIM_HORIZON - reading will begin at the very beginning of the stream
* data used to initialize Kinesis client
    * credentials (aws key, aws secret)
    * region where the stream is located

In case when you want to set up Kinesis client by your own (for example if you're using
more sophisticated authorization methods like Amazon STS, etc.) you can do it by
implementing KinesisClientProvider class:

    public class MyCustomKinesisClientProvider implements KinesisClientProvider {
        @Override
        public AmazonKinesis get() {
            // set up your client here
        }
    }

Usage is pretty straightforward:

    p.apply(KinesisIO.Read.
            from("streamName", InitialPositionInStream.LATEST).
            using(MyCustomKinesisClientProvider()).
    apply( ... ) // other transformations
    
There’s also possibility to start reading using arbitrary point in time - in this case you need to provide java.util.Date object:

    p.apply(KinesisIO.Read.
            from("streamName", date).
            using(MyCustomKinesisClientProvider()).
    apply( ... ) // other transformations
