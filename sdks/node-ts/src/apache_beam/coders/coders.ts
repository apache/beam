import { Writer, Reader } from "protobufjs";
import * as runnerApi from "../proto/beam_runner_api";
import { PipelineContext } from "../base";

interface Class<T> {
  new (...args: any[]): T;
}


/**
 * A registry of coder classes.
 * 
 * Coder classes are registered by their URN. Most coders have a URN of the type `"beam:coder:{coder_name}:v1"`,
 * and they can register a constructor that takes zero or more parameters as input.
 * Constructor input parameters for a coder are usually internal coders that represent sub-components
 * of the enclosing coder (e.g. a coder of key-value pairs (a.k.a. `KVCoder`) may receive the coder
 * for the key and the coder for the value as parameters).
 */
class CoderRegistry {
  internal_registry = {};
  get(urn: string): Class<Coder<any>> {
    const constructor: Class<Coder<any>> = this.internal_registry[urn];
    if (constructor === undefined) {
      throw new Error("Could not find coder for URN " + urn);
    }
    return constructor;
  }

  register(urn: string, coderClass: Class<Coder<any>>) {
    this.internal_registry[urn] = coderClass;
  }
}
export const CODER_REGISTRY = new CoderRegistry();


/**
 * The context for encoding a PCollection element.
 * For example, for strings of utf8 characters or bytes, `wholeStream` encoding means
 * that the string will be encoded as-is; while `needsDelimiter` encoding means that the
 * string will be encoded prefixed with its length.
 * 
 * ```js
 * coder = new StrUtf8Coder()
 * w1 = new Writer()
 * coder.encode("my string", w, Context.wholeStream)
 * console.log(w1.finish())  // <= Prints the pure byte-encoding of the string
 * w2 = new Writer()
 * coder.encode("my string", w, Context.needsDelimiters)
 * console.log(w2.finish())  // <= Prints a length-prefix string of bytes
 * ```
 */
export enum Context {
  /**
   * Whole stream encoding/decoding means that the encoding/decoding function does not need to worry about
   * delimiting the start and end of the current element in the stream of bytes.
   */
  wholeStream = "wholeStream",
  /**
   * Needs-delimiters encoding means that the encoding of data must be such that when decoding,
   * the coder is able to stop decoding data at the end of the current element.
   */
  needsDelimiters = "needsDelimiters",
}

/**
 * This is the base interface for coders, which are responsible in Apache Beam to encode and decode
 * elements of a PCollection.
 */
export interface Coder<T> {

  /**
   * Encode an element into a stream of bytes.
   * @param element - an element within a PCollection
   * @param writer - a writer that interfaces the coder with the output byte stream.
   * @param context - the context within which the element should be encoded.
   */
  encode(element: T, writer: Writer, context: Context);

  /**
   * Decode an element from an incoming stream of bytes.
   * @param reader - a reader that interfaces the coder with the input byte stream
   * @param context - the context within which the element should be encoded
   */
  decode(reader: Reader, context: Context): T;

  /**
   * Convert this coder into its protocol buffer representation for the Runner API.
   * A coder in protobuf format can be shared with other components such as Beam runners,
   * SDK workers; and reconstructed into its runtime representation if necessary.
   * @param pipelineContext - a context that holds relevant pipeline attributes such as other coders already in the pipeline.
   */
  toProto(pipelineContext: PipelineContext): runnerApi.Coder;
}
