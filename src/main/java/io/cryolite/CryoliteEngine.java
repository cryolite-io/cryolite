package io.cryolite;

import io.cryolite.catalog.CatalogManager;
import io.cryolite.storage.StorageManager;

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
  private final CatalogManager catalogManager;
  private final StorageManager storageManager;
  private volatile boolean closed = false;

  /**
   * Creates a new CryoliteEngine with the given configuration.
   *
   * @param config the engine configuration
   * @throws IllegalArgumentException if config is null
   * @throws Exception if catalog or storage initialization fails
   */
  public CryoliteEngine(CryoliteConfig config) throws Exception {
    if (config == null) {
      throw new IllegalArgumentException("Configuration cannot be null");
    }
    this.config = config;

    // Initialize catalog manager with both catalog and storage options
    // (catalog needs storage config for io-impl)
    this.catalogManager = new CatalogManager(config.getCatalogOptions(), config.getStorageOptions());

    // Initialize storage manager with storage options
    this.storageManager = new StorageManager(config.getStorageOptions());
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
   * Returns the catalog manager.
   *
   * @return the catalog manager
   * @throws IllegalStateException if engine is closed
   */
  public CatalogManager getCatalogManager() {
    if (closed) {
      throw new IllegalStateException("Engine is closed");
    }
    return catalogManager;
  }

  /**
   * Returns the storage manager.
   *
   * @return the storage manager
   * @throws IllegalStateException if engine is closed
   */
  public StorageManager getStorageManager() {
    if (closed) {
      throw new IllegalStateException("Engine is closed");
    }
    return storageManager;
  }

  /**
   * Checks if the engine is healthy (catalog and storage are accessible).
   *
   * @return true if both catalog and storage are healthy, false otherwise
   */
  public boolean isHealthy() {
    if (closed) {
      return false;
    }
    return catalogManager.isHealthy() && storageManager.isHealthy();
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
   * <p>After calling this method, the engine cannot be used anymore. This method is idempotent -
   * calling it multiple times is safe.
   */
  public void close() {
    if (!closed) {
      closed = true;
      // Release resources in reverse order
      storageManager.close();
      catalogManager.close();
    }
  }

  @Override
  public String toString() {
    return "CryoliteEngine{" + "config=" + config + ", closed=" + closed + '}';
  }
}
