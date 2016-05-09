
package cz.seznam.euphoria.core.client.dataset;

/**
 * A window aligned across whole dataset.
 */
public interface AlignedWindow<LABEL> extends Window<Void, LABEL> {

  @Override
  default Void getGroup() {
    return null;
  }

}
