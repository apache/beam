
package cz.seznam.euphoria.core.client.util;

/**
 * Either LEFT or RIGHT element.
 */
public final class Either<LEFT, RIGHT> {
  final LEFT left;
  final RIGHT right;
  public static <LEFT, RIGHT> Either<LEFT, RIGHT> left(LEFT left) {
    return new Either<>(left, (RIGHT) null);
  }
  public static <LEFT, RIGHT> Either<LEFT, RIGHT> right(RIGHT right) {
    return new Either<>((LEFT) null, right);
  }
  private Either(LEFT left, RIGHT right) {
    this.left = left;
    this.right = right;
  }
  public boolean isLeft() {
    return left != null;
  }
  public boolean isRight() {
    return right != null;
  }
  public LEFT left() {
    return left;
  }
  public RIGHT right() {
    return right;
  }
}

