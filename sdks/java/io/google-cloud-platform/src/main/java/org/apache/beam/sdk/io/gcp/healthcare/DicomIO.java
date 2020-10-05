package org.apache.beam.sdk.io.gcp.healthcare;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class DicomIO {

    /**
     * Read dicom study metadata from a PCollection of resource webpath
     * @return the read
     * @see ReadDicomStudyMetadata
     */
    public static ReadDicomStudyMetadata retrieveStudyMetadata() {
        return new ReadDicomStudyMetadata();
    }

    /** The type ReadDicomStudyMetadata */
    public static class ReadDicomStudyMetadata extends PTransform<PCollection<PubsubMessage>, PCollection<String>> {

        public ReadDicomStudyMetadata() { }

        public static  final TupleTag<String> OUT = new TupleTag<String>() { };

        /**
         * DoFn to fetch the metadata of a study from a Dicom store based on it's location and study id
         */
        static class FetchStudyMetadataFn extends DoFn<PubsubMessage, String> {

            private HealthcareApiClient dicomStore;

            /**
             * Instantiate the healthcare client
             * @throws IOException
             */
            @Setup
            public void instantiateHealthcareClient() throws IOException{
                this.dicomStore = new HttpHealthcareApiClient();
            }

            /**
             * Process The Pub/Sub message
             * @param context The input containing the pub/sub message
             */
            @ProcessElement
            public void processElement(ProcessContext context) {
                PubsubMessage msg = context.element();
                byte[] msgPayload = msg.getPayload();
                try {
                    String dicomWebPath = new String(msgPayload, "UTF-8");
                    String responseData = dicomStore.retrieveStudyMetadata(dicomWebPath);
                    context.output(responseData);
                } catch (Exception e) {
                    // IO exception, unsupported encoding exception
                    System.out.println(e);
                    if (e.getClass() == IOException.class) {}
                    else if (e.getClass() == UnsupportedEncodingException.class) {
                        // possibly never going to happen? pub/sub message body is always utf-8
                    }
                    else {
                        // context.output <- failed message id?
                    }
                }
            }
        }

        @Override
        public PCollection<String> expand(PCollection<PubsubMessage> input) {
            return input.apply(ParDo.of(new FetchStudyMetadataFn()));
        }

    }
}
