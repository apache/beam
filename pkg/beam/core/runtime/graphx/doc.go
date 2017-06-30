// Package graphx provides facilities to help with the serialization of
// pipelines into a serializable graph structure suitable for the worker.
//
// The registry's Register function is used by transform authors to make their
// type's methods available for remote invocation.  The runner then uses the
// registry's Key and Lookup methods to access information supplied by transform
// authors.
//
// The Encode* and Decode* methods are used to handle serialization of both
// regular Go data and the specific Beam data types. The Encode* methods are
// used after pipeline construction to turn the plan into a serializable form
// that can be sent for remote execution. The Decode* methods are used by the
// runner to recover the execution plan from the serialized form.
package graphx
