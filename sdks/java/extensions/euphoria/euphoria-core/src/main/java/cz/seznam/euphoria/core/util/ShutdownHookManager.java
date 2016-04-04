package cz.seznam.euphoria.core.util;

/**
 * The {@link ShutdownHookManager} enables running shutdown hook
 * in a deterministic order, higher priority first.
 * The JVM runs shutdown hooks in a non-deterministic order or in parallel.
 * This class registers a single JVM shutdown hook and run all the shutdown hooks
 * registered to it (to this class) in order based on their priority.
 */
public interface ShutdownHookManager {

  /**
   * Adds a shutdown hook with a priority, the higher the priority the earlier will run.
   * ShutdownHooks with same priority run in a non-deterministic order.
   */
  void  addShutdownHook(Runnable shutdownHook, int priority);

  /**
   * Removes a shutdown hook.
   * @param shutdownHook
   * @return {@code TRUE} if the shutdownHook was registered and removed, {@code FALSE} otherwise.
   */
  boolean  removeShutdownHook(Runnable shutdownHook);
}
