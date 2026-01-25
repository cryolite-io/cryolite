package io.cryolite;

/**
 * CRYOLITE Runs Your Open Lightweight Iceberg Table Engine.
 *
 * <p>CryoliteEngine is the main entry point for the embedded Iceberg table engine. It provides both
 * low-level and high-level APIs for working with Iceberg tables.
 *
 * <p>This is an embedded library - no CLI, no server, no REST service. It is designed to be used
 * directly from Java applications.
 *
 * @since 0.1.0
 */
public class CryoliteEngine {

  private final CryoliteConfig config;
  private volatile boolean closed = false;

  /**
   * Creates a new CryoliteEngine with the given configuration.
   *
   * @param config the engine configuration
   * @throws IllegalArgumentException if config is null
   */
  public CryoliteEngine(CryoliteConfig config) {
    if (config == null) {
      throw new IllegalArgumentException("Configuration cannot be null");
    }
    this.config = config;
  }

  /**
   * Returns the engine configuration.
   *
   * @return the configuration
   */
  public CryoliteConfig getConfig() {
    return config;
  }

  /**
   * Checks if the engine is closed.
   *
   * @return true if closed, false otherwise
   */
  public boolean isClosed() {
    return closed;
  }

  /**
   * Closes the engine and releases all resources.
   *
   * <p>After calling this method, the engine cannot be used anymore.
   */
  public void close() {
    if (!closed) {
      closed = true;
      // TODO: Release resources (catalog, storage, etc.)
    }
  }

  @Override
  public String toString() {
    return "CryoliteEngine{" + "config=" + config + ", closed=" + closed + '}';
  }
}
