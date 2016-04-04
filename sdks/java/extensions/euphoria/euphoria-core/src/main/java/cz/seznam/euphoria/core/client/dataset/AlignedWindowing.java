
package cz.seznam.euphoria.core.client.dataset;

/**
 * Windowing with windows aligned across the whole dataset.
 */
public interface AlignedWindowing<T, W extends AlignedWindow<W>>
    extends Windowing<T, Void, W> {

 
}
