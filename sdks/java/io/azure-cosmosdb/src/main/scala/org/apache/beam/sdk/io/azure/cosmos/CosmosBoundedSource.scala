package org.apache.beam.sdk.io.azure.cosmos

import org.apache.beam.sdk.coders.{Coder, SerializableCoder}
import org.apache.beam.sdk.io.BoundedSource
import org.apache.beam.sdk.options.PipelineOptions
import org.bson.Document

import java.util
import java.util.Collections

class CosmosBoundedSource(val readCosmos: CosmosRead) extends BoundedSource[Document] {

  /** @inheritDoc
   * TODO: You have to find a better way, maybe by partition key */
  override def split(desiredBundleSizeBytes: Long, options: PipelineOptions): util.List[CosmosBoundedSource] = Collections.singletonList(this)

  /** @inheritDoc
   * The Cosmos DB Coro (SQL) API not support this metrics by the querys */
  override def getEstimatedSizeBytes(options: PipelineOptions): Long = 0L

  override def getOutputCoder: Coder[Document] = SerializableCoder.of(classOf[Document])

  override def createReader(options: PipelineOptions): BoundedSource.BoundedReader[Document] =
    new CosmosBoundedReader(this)
}
