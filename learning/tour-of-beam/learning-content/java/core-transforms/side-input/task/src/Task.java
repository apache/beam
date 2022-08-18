import java.util.Map;
import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

public class Task {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        // List of elements
        PCollection<KV<String, String>> citiesToCountries =
                pipeline.apply("Cities and Countries",
                        Create.of(
                                KV.of("Beijing", "China"),
                                KV.of("London", "United Kingdom"),
                                KV.of("San Francisco", "United States"),
                                KV.of("Singapore", "Singapore"),
                                KV.of("Sydney", "Australia")
                        ));

        PCollectionView<Map<String, String>> citiesToCountriesView =
                createView(citiesToCountries);

        PCollection<Person> persons =
                pipeline.apply("Persons",
                        Create.of(
                                new Person("Henry", "Singapore"),
                                new Person("Jane", "San Francisco"),
                                new Person("Lee", "Beijing"),
                                new Person("John", "Sydney"),
                                new Person("Alfred", "London")
                        ));

        // The applyTransform() converts [persons] and [citiesToCountriesView] to [output]
        PCollection<Person> output = applyTransform(persons, citiesToCountriesView);

        output.apply(Log.ofElements());

        pipeline.run();
    }

    // Create from citiesToCountries view "citiesToCountriesView"
    static PCollectionView<Map<String, String>> createView(
            PCollection<KV<String, String>> citiesToCountries) {

        return citiesToCountries.apply(View.asMap());
    }


    static PCollection<Person> applyTransform(
            PCollection<Person> persons, PCollectionView<Map<String, String>> citiesToCountriesView) {

        return persons.apply(ParDo.of(new DoFn<Person, Person>() {

            // Get city from person and get from city view
            @ProcessElement
            public void processElement(@Element Person person, OutputReceiver<Person> out,
                                       ProcessContext context) {
                Map<String, String> citiesToCountries = context.sideInput(citiesToCountriesView);
                String city = person.getCity();
                String country = citiesToCountries.get(city);

                out.output(new Person(person.getName(), city, country));
            }

        }).withSideInputs(citiesToCountriesView));
    }

}