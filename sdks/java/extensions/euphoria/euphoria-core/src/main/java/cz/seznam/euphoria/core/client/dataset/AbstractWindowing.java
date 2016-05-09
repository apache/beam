
package cz.seznam.euphoria.core.client.dataset;

/**
 * An abstract state aware windowing.
 */
public abstract class AbstractWindowing<T, GROUP, LABEL, W extends Window<GROUP, LABEL>>
    implements Windowing<T, GROUP, LABEL, W>
{

}
