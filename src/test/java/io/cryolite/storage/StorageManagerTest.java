package io.cryolite.storage;

import static org.junit.jupiter.api.Assertions.*;

import io.cryolite.TestConfig;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests for StorageManager. */
class StorageManagerTest {

  @Test
  void testStorageManagerCreation() throws Exception {
    Map<String, String> options = new HashMap<>();
    StorageManager manager =
        new StorageManager(
            TestConfig.getMinioEndpoint(),
            TestConfig.getMinioAccessKey(),
            TestConfig.getMinioSecretKey(),
            TestConfig.getMinioWarehousePath(),
            options);

    assertNotNull(manager);
    assertFalse(manager.isClosed());
    assertNotNull(manager.getFileIO());
    assertEquals(TestConfig.getMinioWarehousePath(), manager.getWarehousePath());
    manager.close();
  }

  @Test
  void testStorageManagerNullEndpoint() {
    Map<String, String> options = new HashMap<>();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new StorageManager(
                null,
                TestConfig.getMinioAccessKey(),
                TestConfig.getMinioSecretKey(),
                TestConfig.getMinioWarehousePath(),
                options));
  }

  @Test
  void testStorageManagerEmptyEndpoint() {
    Map<String, String> options = new HashMap<>();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new StorageManager(
                "",
                TestConfig.getMinioAccessKey(),
                TestConfig.getMinioSecretKey(),
                TestConfig.getMinioWarehousePath(),
                options));
  }

  @Test
  void testStorageManagerNullWarehousePath() {
    Map<String, String> options = new HashMap<>();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new StorageManager(
                TestConfig.getMinioEndpoint(),
                TestConfig.getMinioAccessKey(),
                TestConfig.getMinioSecretKey(),
                null,
                options));
  }

  @Test
  void testStorageManagerEmptyWarehousePath() {
    Map<String, String> options = new HashMap<>();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new StorageManager(
                TestConfig.getMinioEndpoint(),
                TestConfig.getMinioAccessKey(),
                TestConfig.getMinioSecretKey(),
                "",
                options));
  }

  @Test
  void testStorageManagerClose() throws Exception {
    Map<String, String> options = new HashMap<>();
    StorageManager manager =
        new StorageManager(
            TestConfig.getMinioEndpoint(),
            TestConfig.getMinioAccessKey(),
            TestConfig.getMinioSecretKey(),
            TestConfig.getMinioWarehousePath(),
            options);

    assertFalse(manager.isClosed());
    manager.close();
    assertTrue(manager.isClosed());
  }

  @Test
  void testStorageManagerCloseIdempotent() throws Exception {
    Map<String, String> options = new HashMap<>();
    StorageManager manager =
        new StorageManager(
            TestConfig.getMinioEndpoint(),
            TestConfig.getMinioAccessKey(),
            TestConfig.getMinioSecretKey(),
            TestConfig.getMinioWarehousePath(),
            options);

    manager.close();
    assertTrue(manager.isClosed());
    // Calling close again should be safe
    manager.close();
    assertTrue(manager.isClosed());
  }

  @Test
  void testStorageManagerGetFileIOWhenClosed() throws Exception {
    Map<String, String> options = new HashMap<>();
    StorageManager manager =
        new StorageManager(
            TestConfig.getMinioEndpoint(),
            TestConfig.getMinioAccessKey(),
            TestConfig.getMinioSecretKey(),
            TestConfig.getMinioWarehousePath(),
            options);
    manager.close();

    assertThrows(IllegalStateException.class, manager::getFileIO);
  }

  @Test
  void testStorageManagerGetWarehousePathWhenClosed() throws Exception {
    Map<String, String> options = new HashMap<>();
    StorageManager manager =
        new StorageManager(
            TestConfig.getMinioEndpoint(),
            TestConfig.getMinioAccessKey(),
            TestConfig.getMinioSecretKey(),
            TestConfig.getMinioWarehousePath(),
            options);
    manager.close();

    assertThrows(IllegalStateException.class, manager::getWarehousePath);
  }

  @Test
  void testStorageManagerIsHealthyWhenClosed() throws Exception {
    Map<String, String> options = new HashMap<>();
    StorageManager manager =
        new StorageManager(
            TestConfig.getMinioEndpoint(),
            TestConfig.getMinioAccessKey(),
            TestConfig.getMinioSecretKey(),
            TestConfig.getMinioWarehousePath(),
            options);
    manager.close();

    assertFalse(manager.isHealthy());
  }
}
