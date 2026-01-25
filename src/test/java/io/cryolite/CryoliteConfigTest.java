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
}
