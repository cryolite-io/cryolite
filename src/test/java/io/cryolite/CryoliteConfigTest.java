package io.cryolite;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Tests for CryoliteConfig. */
class CryoliteConfigTest {

  @Test
  void testDefaultConfig() {
    CryoliteConfig config = new CryoliteConfig.Builder().build();

    assertEquals("rest", config.getCatalogType());
    assertEquals("s3", config.getStorageType());
    assertTrue(config.getCatalogOptions().isEmpty());
    assertTrue(config.getStorageOptions().isEmpty());
  }

  @Test
  void testConfigWithOptions() {
    CryoliteConfig config =
        new CryoliteConfig.Builder()
            .catalogType("rest")
            .catalogOption("uri", "http://localhost:8181")
            .storageType("s3")
            .storageOption("endpoint", "http://localhost:9000")
            .storageOption("bucket", "cryolite-warehouse")
            .build();

    assertEquals("rest", config.getCatalogType());
    assertEquals("s3", config.getStorageType());
    assertEquals("http://localhost:8181", config.getCatalogOptions().get("uri"));
    assertEquals("http://localhost:9000", config.getStorageOptions().get("endpoint"));
    assertEquals("cryolite-warehouse", config.getStorageOptions().get("bucket"));
  }

  @Test
  void testConfigEquality() {
    CryoliteConfig config1 = new CryoliteConfig.Builder().catalogType("rest").build();
    CryoliteConfig config2 = new CryoliteConfig.Builder().catalogType("rest").build();

    assertEquals(config1, config2);
    assertEquals(config1.hashCode(), config2.hashCode());
  }

  @Test
  void testConfigToString() {
    CryoliteConfig config = new CryoliteConfig.Builder().build();
    String str = config.toString();

    assertNotNull(str);
    assertTrue(str.contains("CryoliteConfig"));
    assertTrue(str.contains("rest"));
  }

  @Test
  void testConfigImmutability() {
    CryoliteConfig config = new CryoliteConfig.Builder().catalogOption("key", "value").build();

    var options = config.getCatalogOptions();
    options.put("newKey", "newValue");

    // Original config should not be modified
    assertFalse(config.getCatalogOptions().containsKey("newKey"));
  }

  @Test
  void testEngineOptions() {
    CryoliteConfig config =
        new CryoliteConfig.Builder()
            .engineOption("parallelism", "8")
            .engineOption("timeout", "30000")
            .build();

    assertEquals("8", config.getEngineOptions().get("parallelism"));
    assertEquals("30000", config.getEngineOptions().get("timeout"));
  }

  @Test
  void testEngineOptionsImmutability() {
    CryoliteConfig config = new CryoliteConfig.Builder().engineOption("key", "value").build();

    var options = config.getEngineOptions();
    options.put("newKey", "newValue");

    // Original config should not be modified
    assertFalse(config.getEngineOptions().containsKey("newKey"));
  }

  @Test
  void testConfigInequality() {
    CryoliteConfig config1 = new CryoliteConfig.Builder().catalogType("rest").build();
    CryoliteConfig config2 = new CryoliteConfig.Builder().catalogType("hive").build();

    assertNotEquals(config1, config2);
    assertNotEquals(config1.hashCode(), config2.hashCode());
  }

  @Test
  void testConfigNotEqualToOtherType() {
    CryoliteConfig config = new CryoliteConfig.Builder().build();

    assertNotEquals(config, "not a config");
    assertNotEquals(config, null);
  }
}
