package io.cryolite.catalog;

import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
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

  private static final String CREDENTIAL_KEY = "credential";
  private static final String SCOPE_KEY = "scope";
  private static final String WAREHOUSE_KEY = "warehouse";

  private final Catalog catalog;
  private volatile boolean closed = false;

  /**
   * Creates a CatalogManager with a REST catalog.
   *
   * @param catalogOptions catalog configuration options (uri, credential, scope, warehouse, etc.)
   * @param storageOptions storage configuration options (io-impl and backend-specific options)
   * @throws IllegalArgumentException if required parameters are missing or invalid
   */
  public CatalogManager(Map<String, String> catalogOptions, Map<String, String> storageOptions) {
    if (catalogOptions == null) {
      throw new IllegalArgumentException("Catalog options cannot be null");
    }
    if (storageOptions == null) {
      throw new IllegalArgumentException("Storage options cannot be null");
    }

    // Extract URI with default
    String catalogUri = catalogOptions.getOrDefault("uri", "http://localhost:8181/api/catalog");
    if (catalogUri == null || catalogUri.isEmpty()) {
      throw new IllegalArgumentException("Catalog URI cannot be null or empty");
    }

    RESTCatalog restCatalog = new RESTCatalog();
    Map<String, String> properties = new java.util.HashMap<>(catalogOptions);
    properties.put("uri", catalogUri);

    // Get io-impl from storage options (with default)
    String ioImpl = storageOptions.getOrDefault("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
    properties.put("io-impl", ioImpl);

    // Pass through all storage options (for FileIO initialization)
    properties.putAll(storageOptions);

    // Add OAuth credentials if provided
    if (catalogOptions.containsKey(CREDENTIAL_KEY)) {
      properties.put(CREDENTIAL_KEY, catalogOptions.get(CREDENTIAL_KEY));
    }
    if (catalogOptions.containsKey(SCOPE_KEY)) {
      properties.put(SCOPE_KEY, catalogOptions.get(SCOPE_KEY));
    }
    if (catalogOptions.containsKey(WAREHOUSE_KEY)) {
      properties.put(WAREHOUSE_KEY, catalogOptions.get(WAREHOUSE_KEY));
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
      // Try to check if a table exists as a health check
      // This is a lightweight operation that verifies catalog connectivity
      catalog.tableExists(TableIdentifier.of("_health_check", "_health_check"));
      return true;
    } catch (Exception e) {
      // TODO: Replace with proper logging framework later
      System.err.println("CatalogManager health check failed: " + e.getMessage());
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
