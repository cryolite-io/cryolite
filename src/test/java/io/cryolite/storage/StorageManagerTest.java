package io.cryolite.storage;

import static org.junit.jupiter.api.Assertions.*;

import io.cryolite.TestConfig;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Tests for StorageManager. */
class StorageManagerTest {

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

  @Test
  void testStorageManagerIsHealthy() throws Exception {
    Map<String, String> options = new HashMap<>();
    StorageManager manager =
        new StorageManager(
            TestConfig.getMinioEndpoint(),
            TestConfig.getMinioAccessKey(),
            TestConfig.getMinioSecretKey(),
            TestConfig.getMinioWarehousePath(),
            options);

    // With Docker Compose running, test the actual health check against MinIO
    // This executes fileIO.newInputFile() to verify connectivity
    assertTrue(manager.isHealthy(), "Storage should be healthy when MinIO is running");
    manager.close();
  }
}
