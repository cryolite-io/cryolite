package io.cryolite.catalog;

import static org.junit.jupiter.api.Assertions.*;

import io.cryolite.TestConfig;
import io.cryolite.TestEnvironment;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Tests for CatalogManager. */
class CatalogManagerTest {

  @BeforeAll
  static void ensureServicesRunning() throws Exception {
    TestEnvironment.ensureServicesRunning();
  }

  private Map<String, String> createCatalogOptions() {
    Map<String, String> options = new HashMap<>();
    options.put("uri", TestConfig.getPolarisUri());
    options.put(
        "credential", TestConfig.getPolarisClientId() + ":" + TestConfig.getPolarisClientSecret());
    options.put("scope", TestConfig.getPolarisScope());
    options.put("warehouse", TestConfig.getPolarisWarehouse());
    return options;
  }

  private Map<String, String> createStorageOptions() {
    Map<String, String> options = new HashMap<>();
    options.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
    options.put("s3.endpoint", TestConfig.getMinioEndpoint());
    options.put("s3.access-key-id", TestConfig.getMinioAccessKey());
    options.put("s3.secret-access-key", TestConfig.getMinioSecretKey());
    options.put("s3.path-style-access", "true");
    options.put("client.region", "us-west-2");
    options.put("warehouse-path", TestConfig.getMinioWarehousePath());
    return options;
  }

  /**
   * Tests that a CatalogManager can be successfully created with valid options.
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>The manager instance is not null
   *   <li>The manager is not closed after creation
   *   <li>The manager can return a valid Iceberg Catalog instance
   * </ul>
   */
  @Test
  void testCatalogManagerCreation() {
    Map<String, String> catalogOptions = createCatalogOptions();
    Map<String, String> storageOptions = createStorageOptions();
    CatalogManager manager = new CatalogManager(catalogOptions, storageOptions);

    assertNotNull(manager);
    assertFalse(manager.isClosed());
    assertNotNull(manager.getCatalog());
    manager.close();
  }

  /**
   * Tests that creating a CatalogManager with null catalog options throws IllegalArgumentException.
   *
   * <p>This is a defensive programming test to ensure the manager fails fast with a clear error
   * message when given invalid input.
   */
  @Test
  void testCatalogManagerNullCatalogOptions() {
    Map<String, String> storageOptions = createStorageOptions();
    assertThrows(IllegalArgumentException.class, () -> new CatalogManager(null, storageOptions));
  }

  /**
   * Tests that creating a CatalogManager with null storage options throws IllegalArgumentException.
   *
   * <p>This is a defensive programming test to ensure the manager fails fast with a clear error
   * message when given invalid input.
   */
  @Test
  void testCatalogManagerNullStorageOptions() {
    Map<String, String> catalogOptions = createCatalogOptions();
    assertThrows(IllegalArgumentException.class, () -> new CatalogManager(catalogOptions, null));
  }

  /**
   * Tests that creating a CatalogManager with an empty URI throws IllegalArgumentException.
   *
   * <p>This is a negative test that verifies the manager validates required configuration
   * parameters.
   */
  @Test
  void testCatalogManagerEmptyUri() {
    Map<String, String> catalogOptions = createCatalogOptions();
    Map<String, String> storageOptions = createStorageOptions();
    catalogOptions.put("uri", "");
    assertThrows(
        IllegalArgumentException.class, () -> new CatalogManager(catalogOptions, storageOptions));
  }

  /**
   * Tests that closing the CatalogManager properly updates its state.
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>The manager is not closed initially
   *   <li>After calling close(), isClosed() returns true
   * </ul>
   */
  @Test
  void testCatalogManagerClose() {
    Map<String, String> catalogOptions = createCatalogOptions();
    Map<String, String> storageOptions = createStorageOptions();
    CatalogManager manager = new CatalogManager(catalogOptions, storageOptions);

    assertFalse(manager.isClosed());
    manager.close();
    assertTrue(manager.isClosed());
  }

  /**
   * Tests that calling close() multiple times is safe (idempotent operation).
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>The first close() call marks the manager as closed
   *   <li>Subsequent close() calls do not throw exceptions
   *   <li>The manager remains closed after multiple close() calls
   * </ul>
   */
  @Test
  void testCatalogManagerCloseIdempotent() {
    Map<String, String> catalogOptions = createCatalogOptions();
    Map<String, String> storageOptions = createStorageOptions();
    CatalogManager manager = new CatalogManager(catalogOptions, storageOptions);

    manager.close();
    assertTrue(manager.isClosed());
    // Calling close again should be safe
    manager.close();
    assertTrue(manager.isClosed());
  }

  /**
   * Tests that calling getCatalog() on a closed manager throws IllegalStateException.
   *
   * <p>This ensures that the manager fails fast when used incorrectly, preventing potential
   * resource leaks or undefined behavior.
   */
  @Test
  void testCatalogManagerGetCatalogWhenClosed() {
    Map<String, String> catalogOptions = createCatalogOptions();
    Map<String, String> storageOptions = createStorageOptions();
    CatalogManager manager = new CatalogManager(catalogOptions, storageOptions);
    manager.close();

    assertThrows(IllegalStateException.class, manager::getCatalog);
  }

  /**
   * Tests that a closed manager always reports unhealthy status.
   *
   * <p>Verifies that the health check correctly identifies a closed manager as unhealthy,
   * regardless of the state of external services.
   */
  @Test
  void testCatalogManagerIsHealthyWhenClosed() {
    Map<String, String> catalogOptions = createCatalogOptions();
    Map<String, String> storageOptions = createStorageOptions();
    CatalogManager manager = new CatalogManager(catalogOptions, storageOptions);
    manager.close();

    assertFalse(manager.isHealthy());
  }

  /**
   * Tests that the CatalogManager reports healthy status when services are running.
   *
   * <p>This is an integration test that verifies the health check logic against real Polaris
   * service running in Docker Compose. The health check executes catalog.listTables() to verify
   * connectivity.
   */
  @Test
  void testCatalogManagerIsHealthy() {
    Map<String, String> catalogOptions = createCatalogOptions();
    Map<String, String> storageOptions = createStorageOptions();
    CatalogManager manager = new CatalogManager(catalogOptions, storageOptions);

    // With Docker Compose running, test the actual health check against Polaris
    // This executes catalog.listTables() to verify connectivity
    assertTrue(manager.isHealthy(), "Catalog should be healthy when Polaris is running");
    manager.close();
  }
}
