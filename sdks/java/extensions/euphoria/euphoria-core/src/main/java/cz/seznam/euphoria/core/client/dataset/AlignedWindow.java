
package cz.seznam.euphoria.core.client.dataset;

/**
 * A window aligned across whole dataset.
 */
public interface AlignedWindow<W extends AlignedWindow<W>>
    extends Window<Void, W> {

  @Override
  default Void getKey() {
    return null;
  }

}
