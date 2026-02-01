package io.cryolite;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Tests for CryoliteConfig. */
class CryoliteConfigTest {

  /**
   * Tests that a CryoliteConfig created with default settings has sensible defaults.
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>Default catalog type is "rest"
   *   <li>Default storage type is "s3"
   *   <li>No options are set by default
   * </ul>
   */
  @Test
  void testDefaultConfig() {
    CryoliteConfig config = new CryoliteConfig.Builder().build();

    assertEquals("rest", config.getCatalogType());
    assertEquals("s3", config.getStorageType());
    assertTrue(config.getCatalogOptions().isEmpty());
    assertTrue(config.getStorageOptions().isEmpty());
  }

  /**
   * Tests that CryoliteConfig correctly stores and retrieves catalog, storage, and engine options.
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>Catalog options are stored and retrievable
   *   <li>Storage options are stored and retrievable
   *   <li>Multiple options can be set independently
   * </ul>
   */
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

  /**
   * Tests that two CryoliteConfig instances with the same settings are equal.
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>equals() returns true for configs with identical settings
   *   <li>hashCode() returns the same value for equal configs
   * </ul>
   *
   * <p>This is important for using configs as map keys or in collections.
   */
  @Test
  void testConfigEquality() {
    CryoliteConfig config1 = new CryoliteConfig.Builder().catalogType("rest").build();
    CryoliteConfig config2 = new CryoliteConfig.Builder().catalogType("rest").build();

    assertEquals(config1, config2);
    assertEquals(config1.hashCode(), config2.hashCode());
  }

  /**
   * Tests that CryoliteConfig has a meaningful string representation.
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>toString() returns a non-null string
   *   <li>The string contains the class name and key configuration values
   * </ul>
   */
  @Test
  void testConfigToString() {
    CryoliteConfig config = new CryoliteConfig.Builder().build();
    String str = config.toString();

    assertNotNull(str);
    assertTrue(str.contains("CryoliteConfig"));
    assertTrue(str.contains("rest"));
  }

  /**
   * Tests that CryoliteConfig is immutable - modifications to returned option maps don't affect the
   * config.
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>getCatalogOptions() returns a defensive copy
   *   <li>Modifications to the returned map don't affect the original config
   * </ul>
   *
   * <p>This ensures thread-safety and prevents accidental configuration mutations.
   */
  @Test
  void testConfigImmutability() {
    CryoliteConfig config = new CryoliteConfig.Builder().catalogOption("key", "value").build();

    var options = config.getCatalogOptions();
    options.put("newKey", "newValue");

    // Original config should not be modified
    assertFalse(config.getCatalogOptions().containsKey("newKey"));
  }

  /**
   * Tests that engine-specific options can be set and retrieved.
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>Engine options are stored and retrievable
   *   <li>Multiple engine options can be set independently
   * </ul>
   *
   * <p>Engine options control runtime behavior like parallelism and timeouts.
   */
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

  /**
   * Tests that CryoliteConfig engine options are immutable.
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>getEngineOptions() returns a defensive copy
   *   <li>Modifications to the returned map don't affect the original config
   * </ul>
   *
   * <p>This ensures thread-safety for engine options.
   */
  @Test
  void testEngineOptionsImmutability() {
    CryoliteConfig config = new CryoliteConfig.Builder().engineOption("key", "value").build();

    var options = config.getEngineOptions();
    options.put("newKey", "newValue");

    // Original config should not be modified
    assertFalse(config.getEngineOptions().containsKey("newKey"));
  }

  /**
   * Tests that two CryoliteConfig instances with different settings are not equal.
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>equals() returns false for configs with different settings
   *   <li>hashCode() returns different values for unequal configs
   * </ul>
   */
  @Test
  void testConfigInequality() {
    CryoliteConfig config1 = new CryoliteConfig.Builder().catalogType("rest").build();
    CryoliteConfig config2 = new CryoliteConfig.Builder().catalogType("hive").build();

    assertNotEquals(config1, config2);
    assertNotEquals(config1.hashCode(), config2.hashCode());
  }

  /**
   * Tests that CryoliteConfig is not equal to objects of other types.
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>equals() returns false when compared to a String
   *   <li>equals() returns false when compared to null
   * </ul>
   *
   * <p>This is a defensive test for proper equals() implementation.
   */
  @Test
  void testConfigNotEqualToOtherType() {
    CryoliteConfig config = new CryoliteConfig.Builder().build();

    assertNotEquals(config, "not a config");
    assertNotEquals(config, null);
  }
}
