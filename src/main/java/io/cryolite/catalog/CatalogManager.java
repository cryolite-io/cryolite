package io.cryolite.catalog;

import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.RESTCatalog;

/**
 * Manages Iceberg catalog connections.
 *
 * <p>Handles initialization and lifecycle of REST catalog connections to Polaris or other REST
 * catalog implementations.
 *
 * @since 0.1.0
 */
public class CatalogManager {

  private final Catalog catalog;
  private volatile boolean closed = false;

  /**
   * Creates a CatalogManager with a REST catalog.
   *
   * @param catalogUri the REST catalog URI (e.g., http://localhost:8181)
   * @param catalogOptions additional catalog configuration options
   * @throws IllegalArgumentException if catalogUri is null or empty
   */
  public CatalogManager(String catalogUri, Map<String, String> catalogOptions) {
    if (catalogUri == null || catalogUri.isEmpty()) {
      throw new IllegalArgumentException("Catalog URI cannot be null or empty");
    }

    RESTCatalog restCatalog = new RESTCatalog();
    Map<String, String> properties = new java.util.HashMap<>(catalogOptions);
    properties.put("uri", catalogUri);

    // Add OAuth credentials if provided
    if (catalogOptions.containsKey("credential")) {
      properties.put("credential", catalogOptions.get("credential"));
    }
    if (catalogOptions.containsKey("scope")) {
      properties.put("scope", catalogOptions.get("scope"));
    }
    if (catalogOptions.containsKey("warehouse")) {
      properties.put("warehouse", catalogOptions.get("warehouse"));
    }

    restCatalog.initialize("cryolite_catalog", properties);
    this.catalog = restCatalog;
  }

  /**
   * Gets the underlying Iceberg catalog.
   *
   * @return the Catalog instance
   */
  public Catalog getCatalog() {
    if (closed) {
      throw new IllegalStateException("CatalogManager is closed");
    }
    return catalog;
  }

  /**
   * Checks if the catalog is accessible.
   *
   * @return true if catalog is accessible, false otherwise
   */
  public boolean isHealthy() {
    if (closed) {
      return false;
    }
    try {
      // Try to list tables in the root namespace as a health check
      catalog.listTables(Namespace.empty());
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Closes the catalog connection.
   *
   * <p>This method is idempotent - calling it multiple times is safe.
   */
  public void close() {
    if (!closed) {
      closed = true;
      if (catalog instanceof AutoCloseable) {
        try {
          ((AutoCloseable) catalog).close();
        } catch (Exception e) {
          // Log but don't throw
        }
      }
    }
  }

  /**
   * Checks if the catalog manager is closed.
   *
   * @return true if closed, false otherwise
   */
  public boolean isClosed() {
    return closed;
  }
}
