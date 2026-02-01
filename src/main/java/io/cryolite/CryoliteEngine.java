package io.cryolite;

import io.cryolite.catalog.CatalogManager;

/**
 * CRYOLITE Runs Your Open Lightweight Iceberg Table Engine.
 *
 * <p>CryoliteEngine is the main entry point for the embedded Iceberg table engine. It provides
 * access to the Iceberg Catalog for all table and namespace operations.
 *
 * <p>This is an embedded library - no CLI, no server, no REST service. It is designed to be used
 * directly from Java applications.
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * CryoliteEngine engine = new CryoliteEngine(config);
 * Catalog catalog = engine.getCatalog();
 *
 * // Namespace operations (cast to SupportsNamespaces)
 * SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
 * nsCatalog.createNamespace(Namespace.of("my_namespace"), Map.of());
 *
 * // Table operations
 * catalog.createTable(TableIdentifier.of("my_namespace", "my_table"), schema);
 * }</pre>
 *
 * @since 0.1.0
 */
public class CryoliteEngine {

  private final CryoliteConfig config;
  private final CatalogManager catalogManager;
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
    this.catalogManager =
        new CatalogManager(config.getCatalogOptions(), config.getStorageOptions());
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
   * Returns the Iceberg Catalog for table and namespace operations.
   *
   * <p>The returned catalog can be cast to {@code SupportsNamespaces} for namespace operations.
   *
   * @return the Iceberg Catalog
   * @throws IllegalStateException if engine is closed
   */
  public org.apache.iceberg.catalog.Catalog getCatalog() {
    if (closed) {
      throw new IllegalStateException("Engine is closed");
    }
    return catalogManager.getCatalog();
  }

  /**
   * Checks if the engine is healthy (catalog is accessible).
   *
   * @return true if catalog is healthy, false otherwise
   */
  public boolean isHealthy() {
    if (closed) {
      return false;
    }
    return catalogManager.isHealthy();
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
      catalogManager.close();
    }
  }

  @Override
  public String toString() {
    return "CryoliteEngine{" + "config=" + config + ", closed=" + closed + '}';
  }
}
