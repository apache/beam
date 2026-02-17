---
title: "Our Experience at Beam College 2025: 1st Place Hackathon Winners"
date: 2025-07-08
authors:
    - ashukla
    - dkanade
categories:
    - blog
---
<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

## Introduction: The Beam of an Idea
In the world of machine learning for healthcare, preprocessing large pathology image datasets at scale remains a bottleneck. Whole Slide Images (WSIs) in medical imaging can reach massive sizes. Traditional Python tools (PIL, etc.) fail under memory pressure, especially when handling thousands of such high-resolution images. This becomes a bottleneck for ML modeling tasks using standard tools.

Having previously worked on image processing for object detection in machine learning, we also understood how crucial it is to preprocess and structure image data correctly for downstream tasks. These challenges are non-trivial and even more critical in healthcare, making it a natural and high-impact use case for scalable data processing frameworks like Apache Beam.

So, in the [Beam Summit 2025 Hackathon](https://beamcollege.dev/hackathon/), we joined as team "PCollectors" with the goal to leverage Beam to process large image data and convert it to a format suitable for downstream ML tasks. We were amazed to know that we secured 1st place with the implemented solution!

## The Project: Scalable WSI Preprocessing Beam Pipeline
[GitHub Repo](https://github.com/adityashukla8/medical_image_processing_beam)

### The Goal
The primary objective of the pipeline was to process patient data (CSV) & WSIs, extract embeddings, combine the metadata, and output the final dataset in TFRecord format, ready for large-scale ML training.

### Solution Overview
Our pipeline processes:

- Patient metadata (CSV)
- WSI files (.tif)
- Split the images into “tiles”
- Extract filtered image tiles based on the background threshold
- Generate max & avg embeddings per patient using EfficientNet
- Merge metadata + embeddings into TFRecords

All in a scalable, memory-efficient, cloud-native pipeline using Apache Beam and Dataflow.

### Dataset
Source: Mayo Clinic STRIP AI Dataset (Kaggle)
Metadata: Each row = { image_id, center_id, patient_id, image_num, label }
Multiple images per patient
Labels exist only at the patient level
Images:
High-res .tif pathology slides

### Tech Stack
- Apache Beam: Orchestration engine
- Google Cloud Dataflow: Scalable runner
- Google Cloud Storage: Input TIFFs + output TFRecords
- TensorFlow: For embedding generation (EfficientNet) and TFRecord serialization

## The Hackathon Journey
Participating in the hackathon introduced us to multiple new things and allowed us to learn and implement simultaneously. Through the hackathon weekend, we:

- Designed the end-to-end pipeline
- Integrated pyvips + openslide for efficient image loading
- Used Beam's RunInference API with TensorFlow
- Tiled and filtered images
- Wrote patient-level embeddings to TFRecords

## What we Learnt
Apache Beam is really powerful for parallel and cloud-native ML preprocessing.
Dataflow is the go-to tool when processing large data, like medical images

## What’s Next for The Project
Looking ahead, the pipeline can be extended beyond fixed-size tiling by incorporating image segmentation techniques to generate more meaningful patches based on tissue regions. This approach can improve ML model performance by focusing only on relevant areas. Moreover, the same preprocessing framework can be adapted for video data, where frames can be treated as time-indexed image slices, effectively enabling temporal modeling for time-series tasks such as motion analysis or progression tracking. Finally, we plan to adapt this pipeline to multiple downstream use cases for AI in healthcare by combining histology images with genomic data, clinical notes, or radiology scans, paving the way for more comprehensive and context-aware models in biomedical machine learning.

**Project Submission Demo**: [Beam Demo - PCollectors.mp4](https://drive.google.com/file/d/1Os5SvgqHiqfMkoCWOuaVvEPXsnhqXlLx/view?usp=sharing)

## Conclusion
We are ML Engineers, working at [Intuitive.Cloud](www.intuitive.cloud), where we play around with large-scale data to build scalable, efficient, dynamic data processing pipelines that prepare it for downstream ML tasks, with Apache Beam and Google Cloud DataFlow being the central pieces.

Participating in the hackathon was a great learning opportunity, huge thanks to the organizers, mentors, and the Apache Beam community!

\- [Aditya Shukla](https://www.linkedin.com/in/adityashukla8/) & [Darshan Kanade](https://in.linkedin.com/in/darshan-kanade-0797851b3)

