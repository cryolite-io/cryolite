package io.cryolite.catalog;

import static org.junit.jupiter.api.Assertions.*;

import io.cryolite.TestConfig;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Tests for CatalogManager. */
class CatalogManagerTest {

  @BeforeAll
  static void ensureDockerComposeRunning() throws Exception {
    // Ensure Docker Compose services are running before tests
    ProcessBuilder pb = new ProcessBuilder("docker-compose", "up", "-d");
    pb.redirectErrorStream(true);
    Process process = pb.start();
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      throw new RuntimeException("Failed to start Docker Compose services");
    }
    // Give services time to become healthy
    Thread.sleep(5000);
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

  @Test
  void testCatalogManagerNullCatalogOptions() {
    Map<String, String> storageOptions = createStorageOptions();
    assertThrows(IllegalArgumentException.class, () -> new CatalogManager(null, storageOptions));
  }

  @Test
  void testCatalogManagerNullStorageOptions() {
    Map<String, String> catalogOptions = createCatalogOptions();
    assertThrows(IllegalArgumentException.class, () -> new CatalogManager(catalogOptions, null));
  }

  @Test
  void testCatalogManagerEmptyUri() {
    Map<String, String> catalogOptions = createCatalogOptions();
    Map<String, String> storageOptions = createStorageOptions();
    catalogOptions.put("uri", "");
    assertThrows(
        IllegalArgumentException.class, () -> new CatalogManager(catalogOptions, storageOptions));
  }

  @Test
  void testCatalogManagerClose() {
    Map<String, String> catalogOptions = createCatalogOptions();
    Map<String, String> storageOptions = createStorageOptions();
    CatalogManager manager = new CatalogManager(catalogOptions, storageOptions);

    assertFalse(manager.isClosed());
    manager.close();
    assertTrue(manager.isClosed());
  }

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

  @Test
  void testCatalogManagerGetCatalogWhenClosed() {
    Map<String, String> catalogOptions = createCatalogOptions();
    Map<String, String> storageOptions = createStorageOptions();
    CatalogManager manager = new CatalogManager(catalogOptions, storageOptions);
    manager.close();

    assertThrows(IllegalStateException.class, manager::getCatalog);
  }

  @Test
  void testCatalogManagerIsHealthyWhenClosed() {
    Map<String, String> catalogOptions = createCatalogOptions();
    Map<String, String> storageOptions = createStorageOptions();
    CatalogManager manager = new CatalogManager(catalogOptions, storageOptions);
    manager.close();

    assertFalse(manager.isHealthy());
  }

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
