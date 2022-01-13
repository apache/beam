import { PTransform } from "./transform";
import { Impulse } from "./internal";
import { Root, PCollection } from "../pvalue";

/**
 * A Ptransform that represents a 'static' source with a list of elements passed at construction time. It
 * returns a PCollection that contains the elements in the input list.
 *
 * @extends PTransform
 */
export class Create<T> extends PTransform<Root, PCollection<T>> {
  elements: T[];

  /**
   * Construct a new Create PTransform.
   * @param elements - the list of elements in the PCollection
   */
  constructor(elements: T[]) {
    super("Create");
    this.elements = elements;
  }

  expand(root: Root) {
    const this_ = this;
    // TODO: Store encoded values and conditionally shuffle.
    return root.apply(new Impulse()).flatMap(function* (_) {
      yield* this_.elements;
    });
  }
}
