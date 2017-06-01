// Package beam provides a simple, powerful model for building both batch and
// streaming parallel data processing pipelines.
//
// TODO(herohde) 6/2/2017: add short overview of each important concept (multiple
// paragraphs), how they fit together, and an example of a working pipeline.
//
// Here is a typical example of use:
//
//    // Create an empty pipeline
//    p := beam.NewPipeline()
//
//    // A root PTransform, like textio.Read, gets added to the Pipeline by
//    // being applied:
//    lines := textio.Read(p, "protocol://path/file*.txt")
//
//    // A Pipeline can have multiple root transforms:
//    moreLines :=  textio.Read(p, "protocol://other/path/file*.txt")
//
//    // Further PTransforms can be applied, in an arbitrary (acyclic) graph.
//    // Subsequent PTransforms (and intermediate PCollections etc.) are
//    // implicitly part of the same Pipeline.
//    all := beam.Flatten(p, lines, moreLines)
//    words := beam.ParDo(p, func (line string, emit func(string))) {
//         for _, s := range strings.FieldsFunc(s, unicode.IsPunct) {
//             emit(strings.TrimSpace(s)))
//         }
//    }, all)
//    formatted := beam.ParDo(p, string.ToUpper, words)
//    textio.Write(p, "protocol://output/path", formatted)
//
//    // Primitive PTransforms aren't executed when they're applied, rather
//    // they're just added to the Pipeline.  Once the whole Pipeline of
//    // PTransforms is constructed, the Pipeline's PTransforms can be run
//    // using a PipelineRunner.  The local runner executes the Pipeline
//    // directly, sequentially, in this one process, which is useful for
//    // unit tests and simple experiments:
//    if err := local.Run(context.Background(), p); err != nil {
//        log.Fatalf("Pipeline failed: %v", err)
//    }
//
// See: https://beam.apache.org/documentation/programming-guide
package beam
