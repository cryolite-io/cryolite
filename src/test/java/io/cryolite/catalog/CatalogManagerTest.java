package io.cryolite.catalog;

import static org.junit.jupiter.api.Assertions.*;

import io.cryolite.TestConfig;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests for CatalogManager. */
class CatalogManagerTest {

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
}
