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

  private Map<String, String> createTestOptions() {
    Map<String, String> options = new HashMap<>();
    options.put(
        "credential", TestConfig.getPolarisClientId() + ":" + TestConfig.getPolarisClientSecret());
    options.put("scope", TestConfig.getPolarisScope());
    options.put("warehouse", TestConfig.getPolarisWarehouse());
    return options;
  }

  @Test
  void testCatalogManagerCreation() {
    Map<String, String> options = createTestOptions();
    CatalogManager manager = new CatalogManager(TestConfig.getPolarisUri(), options);

    assertNotNull(manager);
    assertFalse(manager.isClosed());
    assertNotNull(manager.getCatalog());
    manager.close();
  }

  @Test
  void testCatalogManagerNullUri() {
    Map<String, String> options = new HashMap<>();
    assertThrows(IllegalArgumentException.class, () -> new CatalogManager(null, options));
  }

  @Test
  void testCatalogManagerEmptyUri() {
    Map<String, String> options = new HashMap<>();
    assertThrows(IllegalArgumentException.class, () -> new CatalogManager("", options));
  }

  @Test
  void testCatalogManagerClose() {
    Map<String, String> options = createTestOptions();
    CatalogManager manager = new CatalogManager(TestConfig.getPolarisUri(), options);

    assertFalse(manager.isClosed());
    manager.close();
    assertTrue(manager.isClosed());
  }

  @Test
  void testCatalogManagerCloseIdempotent() {
    Map<String, String> options = createTestOptions();
    CatalogManager manager = new CatalogManager(TestConfig.getPolarisUri(), options);

    manager.close();
    assertTrue(manager.isClosed());
    // Calling close again should be safe
    manager.close();
    assertTrue(manager.isClosed());
  }

  @Test
  void testCatalogManagerGetCatalogWhenClosed() {
    Map<String, String> options = createTestOptions();
    CatalogManager manager = new CatalogManager(TestConfig.getPolarisUri(), options);
    manager.close();

    assertThrows(IllegalStateException.class, manager::getCatalog);
  }

  @Test
  void testCatalogManagerIsHealthyWhenClosed() {
    Map<String, String> options = createTestOptions();
    CatalogManager manager = new CatalogManager(TestConfig.getPolarisUri(), options);
    manager.close();

    assertFalse(manager.isHealthy());
  }

  @Test
  void testCatalogManagerIsHealthy() {
    Map<String, String> options = createTestOptions();
    CatalogManager manager = new CatalogManager(TestConfig.getPolarisUri(), options);

    // With Docker Compose running, test the actual health check against Polaris
    // This executes catalog.listTables() to verify connectivity
    assertTrue(manager.isHealthy(), "Catalog should be healthy when Polaris is running");
    manager.close();
  }
}
