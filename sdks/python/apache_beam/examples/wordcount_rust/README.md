This directory contains an example of a Python pipeline that uses Rust DoFns to perform some of the string processing in wordcount. This is performed using [PyO3](https://pyo3.rs/v0.27.2/) to produce bindings for the Rust code, managed using the [maturin](https://github.com/PyO3/maturin) python package. 

This example should be built and run in a Python virtual environment with Apache Beam and maturin installed. The `requirements.txt` file in this directory can be used to install the version of maturin used when the example was created.

To build the Rust code, run the following from the wordcount_rust directory:

```bash
cd ./word_processing
maturin develop
```

This will compile the Rust code and build a Python package linked to it in the current environment. The resulting package can be imported as a Python module called `word_processing`.

To execute wordcount locally using the direct runner, execute the following from the wordcount_rust directory within the same virtual environment:

```bash
python wordcount.py --runner DirectRunner --input * --output counts.txt
```

To execute wordcount using the Dataflow runner, the tarball of the PyO3 Rust package must be provided to GCP. This is done by building the tarball then providing it as an `extra_package` argument. The tarball can be built using the following command from the wordcount_rust directory:

```bash
cd ./word_processing
python -m build --sdist
```
This places the tarball in `./word_processing/dist` as `word_processing-0.1.0.tar.gz`. Job submission to Dataflow from the `wordcount_rust` directory then looks like the following:

```bash
python wordcount.py --runner DataflowRunner --input gs://apache-beam-samples/shakespeare/*.txt --output gs://<YOUR_BUCKET>/wordcount_rust/counts.txt --project <YOUR_PROJECT> --region <YOUR_REGION> --extra_package ./word_processing/dist/word_processing-0.1.0.tar.gz
```

The job will then execute on Dataflow, installing the Rust package during worker setup. Wordcount will then execute and produce a counts.txt file in the specified output bucket.