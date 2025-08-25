---
title: "Milvus I/O connector"
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

# Milvus I/O connector

The Beam SDKs include built-in transforms that can write data to [Milvus](https://milvus.io/) vector databases. Milvus is a high-performance, cloud-native vector database designed for machine learning and AI applications.

## Before you start

To use MilvusIO, you need to install the required dependencies. The Milvus I/O connector is part of the ML/RAG functionality in Apache Beam.

```python
pip install apache-beam[milvus,gcp]
```

**Additional resources:**

* [MilvusIO source code](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/ml/rag/ingestion/milvus_search.py)
* [MilvusIO Pydoc](https://beam.apache.org/releases/pydoc/current/apache_beam.ml.rag.ingestion.milvus_search.html)
* [Milvus Documentation](https://milvus.io/docs)

## Overview

The Milvus I/O connector provides a sink for writing vector embeddings and
associated metadata to Milvus collections. This connector is specifically
designed for RAG (Retrieval-Augmented Generation) use cases where you need to
store document chunks with their vector embeddings for similarity search.

### Key Features

- **Vector Database Integration**: Write embeddings and metadata to Milvus collections
- **RAG-Optimized**: Built specifically for RAG workflows with document chunks
- **Batch Processing**: Efficient batched writes to optimize performance
- **Flexible Schema**: Configurable column mappings for different data schemas
- **Connection Management**: Proper connection lifecycle management with context managers

## Writing to Milvus

### Basic Usage

```python
import apache_beam as beam
from apache_beam.ml.rag.ingestion.milvus_search import MilvusVectorWriterConfig
from apache_beam.ml.rag.utils import MilvusConnectionConfig

# Configure connection to Milvus.
connection_config = MilvusConnectionConfig(
    uri="http://localhost:19530",  # Milvus server URI
    db_name="default"              # Database name
)

# Configure write settings.
write_config = MilvusWriteConfig(
    collection_name="document_embeddings",
    write_batch_size=1000
)

# Create the writer configuration.
milvus_config = MilvusVectorWriterConfig(
    connection_params=connection_config,
    write_config=write_config
)

# Use in a pipeline.
with beam.Pipeline() as pipeline:
    chunks = (
        pipeline
        | "Read Data" >> beam.io.ReadFromText("input.txt")
        | "Process to Chunks" >> beam.Map(process_to_chunks)
    )

    # Write to Milvus.
    chunks | "Write to Milvus" >> milvus_config.create_write_transform()
```

### Configuration Options

#### Connection Configuration

```python
from apache_beam.ml.rag.utils import MilvusConnectionConfig

connection_config = MilvusConnectionConfig(
    uri="http://localhost:19530",     # Milvus server URI
    token="your_token",               # Authentication token (optional)
    db_name="vector_db",              # Database name
    timeout=30.0                      # Connection timeout in seconds
)
```

#### Write Configuration

```python
from apache_beam.ml.rag.ingestion.milvus_search import MilvusWriteConfig

write_config = MilvusWriteConfig(
    collection_name="embeddings",     # Target collection name
    partition_name="",                # Partition name (optional)
    timeout=60.0,                     # Write operation timeout
    write_batch_size=1000             # Number of records per batch
)
```

### Working with Chunks

The Milvus I/O connector is designed to work with `Chunk` objects that contain
document content and embeddings:

```python
from apache_beam.ml.rag.types import Chunk
import numpy as np

def create_chunk_example():
    return Chunk(
        id="doc_1_chunk_1",
        content="This is the document content...",
        embedding=[0.1, 0.2, 0.3, 0.4, 0.5],  # Dense embedding vector
        sparse_embedding={"token_1": 0.5, "token_2": 0.3},  # Sparse embedding (optional)
        metadata={"source": "document.pdf", "page": 1}
    )
```

### Custom Column Specifications

You can customize how chunk fields are mapped to Milvus collection fields:

```python
from apache_beam.ml.rag.ingestion.postgres_common import ColumnSpec

# Define custom column mappings.
custom_column_specs = [
    ColumnSpec(
        column_name="doc_id",
        value_fn=lambda chunk: chunk.id
    ),
    ColumnSpec(
        column_name="vector",
        value_fn=lambda chunk: list(chunk.embedding)
    ),
    ColumnSpec(
        column_name="text_content",
        value_fn=lambda chunk: chunk.content
    ),
    ColumnSpec(
        column_name="document_metadata",
        value_fn=lambda chunk: dict(chunk.metadata)
    )
]

# Use custom column specs.
milvus_config = MilvusVectorWriterConfig(
    connection_params=connection_config,
    write_config=write_config,
    column_specs=custom_column_specs
)
```

## Complete Example

Here's a complete example that processes documents and writes them to Milvus:

```python
import apache_beam as beam
from apache_beam.ml.rag.ingestion.milvus_search import (
    MilvusVectorWriterConfig,
    MilvusWriteConfig
)
from apache_beam.ml.rag.utils import MilvusConnectionConfig
from apache_beam.ml.rag.types import Chunk
import numpy as np

def process_document(document_text):
    """Process a document into chunks with embeddings."""
    # This is a simplified example - in practice you would:
    # 1. Split document into chunks
    # 2. Generate embeddings using a model
    # 3. Extract metadata

    chunks = []
    sentences = document_text.split('.')

    for i, sentence in enumerate(sentences):
        if sentence.strip():
            # Generate mock embedding (replace with real embedding model).
            embedding = np.random.rand(384).tolist()  # 384-dimensional vector

            chunk = Chunk(
                id=f"doc_chunk_{i}",
                content=sentence.strip(),
                embedding=embedding,
                metadata={"chunk_index": i, "length": len(sentence)}
            )
            chunks.append(chunk)

    return chunks

def run_pipeline():
    # Configure Milvus connection.
    connection_config = MilvusConnectionConfig(
        uri="http://localhost:19530",
        db_name="rag_database"
    )

    # Configure write settings.
    write_config = MilvusWriteConfig(
        collection_name="document_chunks",
        write_batch_size=500
    )

    # Create writer configuration.
    milvus_config = MilvusVectorWriterConfig(
        connection_params=connection_config,
        write_config=write_config
    )

    # Define pipeline.
    with beam.Pipeline() as pipeline:
        documents = (
            pipeline
            | "Create Sample Documents" >> beam.Create([
                "First document content. It has multiple sentences.",
                "Second document with different content. More sentences here."
            ])
        )

        chunks = (
            documents
            | "Process Documents" >> beam.FlatMap(process_document)
        )

        # Write to Milvus.
        chunks | "Write to Milvus" >> milvus_config.create_write_transform()

if __name__ == "__main__":
    run_pipeline()
```

## Performance Considerations

### Batch Size Optimization

The write batch size significantly affects performance. Larger batches reduce
the number of network round-trips but consume more memory:

```python
# For high-throughput scenarios.
write_config = MilvusWriteConfig(
    collection_name="large_collection",
    write_batch_size=2000  # Larger batches for better throughput
)

# For memory-constrained environments.
write_config = MilvusWriteConfig(
    collection_name="small_collection",
    write_batch_size=100   # Smaller batches to reduce memory usage
)
```

### Production Configuration

For production deployments, consider using appropriate timeout settings and
connection parameters:

```python
connection_config = MilvusConnectionConfig(
    uri="http://milvus-cluster:19530",
    timeout=120.0,  # Longer timeout for production workloads
    db_name="production_db",
    token="your_production_token"  # Using authentication in production
)
```

## Error Handling

The connector includes built-in error handling and logging. Monitor your
pipeline logs for any connection or write failures:

```python
import logging

# Enable debug logging to see detailed operation information.
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# In your processing function.
def safe_process_document(document):
    try:
        return process_document(document)
    except Exception as e:
        logger.error(f"Failed to process document: {e}")
        return []  # Return empty list on failure
```

## Notebook exmaple

<a href="https://colab.research.google.com/github/apache/beam/blob/master/examples/notebooks/beam-ml/milvus_vector_ingestion_and_search.ipynb" target="_blank">
  <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab" width="150" height="auto" style="max-width: 100%"/>
</a>


## Related transforms

- [Milvus Enrichment Handler in Apache Beam](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/ml/rag/enrichment/milvus_search.py)
