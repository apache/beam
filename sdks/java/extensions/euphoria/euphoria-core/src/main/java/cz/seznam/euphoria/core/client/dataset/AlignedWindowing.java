
package cz.seznam.euphoria.core.client.dataset;

/**
 * Windowing with windows aligned across the whole dataset.
 */
public interface AlignedWindowing<T, LABEL, W extends WindowContext<Void, LABEL>>
    extends Windowing<T, Void, LABEL, W> {

}
