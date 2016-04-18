
package cz.seznam.euphoria.core.client.dataset;

/**
 * A window aligned across whole dataset.
 */
public interface AlignedWindow extends Window<Void> {

  @Override
  default Void getKey() {
    return null;
  }

}
