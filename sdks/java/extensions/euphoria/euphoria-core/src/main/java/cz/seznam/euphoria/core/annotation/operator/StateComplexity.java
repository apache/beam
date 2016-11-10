
package cz.seznam.euphoria.core.annotation.operator;

/**
 * Space complexity of operator's state in each window depending on size of input.
 */
public class StateComplexity {

  /** The size of state will be O(N) in the size of input. */
  public static final int LINEAR = 1;

  /** The size of state will be sub-linear but not constant. */
  public static final int SUBLINEAR = 2;

  /** The size of state will be O(1) in the size of input. */
  public static final int CONSTANT = 3;
  
  /** There is no state in this operator. */
  public static final int ZERO = 4;

  /**
   * The size of state will be O(1) if the passed function
   * is `combinable` (commutative, associative), otherwise it will be O(N).
   */
  public static final int CONSTANT_IF_COMBINABLE = 5;

  // do not construct this object
  private StateComplexity() { }

}
