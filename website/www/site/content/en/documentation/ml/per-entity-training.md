---
title: "Per Entity Training"
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

# Per Entity Training
The aim of this pipeline example is to demonstrate per entity training in Beam. Per entity training refers to the process of training a machine learning model for each individual entity, rather than training a single model for all entities. In this approach, a separate model is trained for each entity based on the data specific to that entity. Per entity training can be beneficial in the following scenarios:

* Having separate models allows for more personalized and tailored predictions for each group. Each group may have different characteristics, patterns, and behaviors that a single large model may not be able to capture effectively.

* Having separate models can also help to reduce the complexity of the overall model and make it more efficient. The overall model would only need to focus on the specific characteristics and patterns of the individual group, rather than trying to account for all possible characteristics and patterns across all groups.

* Having separate models can address issues of bias and fairness. Because a single model trained on a diverse dataset might not generalize well to certain groups, separate models for each group can reduce the impact of bias.

* This approach is often favored in production settings, because it makes it easier to detect issues specific to a limited segment of the overall population.

* When working with smaller models and datasets, the process of training and retraining can be completed more rapidly and efficiently. Both the training and retraining can be done in parallel, reducing the amount of time spent waiting for results. Furthermore, smaller models and datasets also have the advantage of being less resource-intensive, which allows them to be run on less expensive hardware.

## Dataset
This example uses [Adult Census Income dataset](https://archive.ics.uci.edu/ml/datasets/adult). The dataset contains information about individuals, including their demographic characteristics, employment status, and income level. The dataset includes both categorical and numerical features, such as age, education, occupation, and hours worked per week, as well as a binary label indicating whether an individual's income is above or below 50,000 USD. The primary goal of this dataset is to be used for classification tasks, where the model will predict whether an individual's income is above or below a certain threshold based on the provided features.The pipeline expects the `adult.data` CSV file as an input. This file can be downloaded from [here](https://archive.ics.uci.edu/ml/machine-learning-databases/adult/).

### Run the Pipeline
First, install the required packages `apache-beam==2.44.0`, `scikit-learn==1.0.2` and `pandas==1.3.5`.
You can view the code on [GitHub](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/per_entity_training.py).
Use `python per_entity_training.py --input path/to/adult.data`


### Train the pipeline
The pipeline can be broken down into the following main steps:
1. Read the data from the provided input path.
2. Filter the data based on some criteria.
3. Create key based on education level.
4. Group dataset based on the key generated.
5. Preprocess the dataset.
6. Train model per education level.
7. Save the trained models.

The following code snippet contains the detailed steps:

{{< highlight >}}
    with beam.Pipeline(options=pipeline_options) as pipeline:
        _ = (
            pipeline | "Read Data" >> beam.io.ReadFromText(known_args.input)
            | "Split data to make List" >> beam.Map(lambda x: x.split(','))
            | "Filter rows" >> beam.Filter(custom_filter)
            | "Create Key" >> beam.ParDo(CreateKey())
            | "Group by education" >> beam.GroupByKey()
            | "Prepare Data" >> beam.ParDo(PrepareDataforTraining())
            | "Train Model" >> beam.ParDo(TrainModel())
            |
            "Save" >> fileio.WriteToFiles(path=known_args.output, sink=ModelSink()))
{{< /highlight >}}
