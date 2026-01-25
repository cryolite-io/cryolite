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

  private Map<String, String> createTestOptions() {
    Map<String, String> options = new HashMap<>();
    // Specify FileIO implementation
    options.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
    // Use S3FileIO property names directly
    options.put("s3.endpoint", TestConfig.getMinioEndpoint());
    options.put("s3.access-key-id", TestConfig.getMinioAccessKey());
    options.put("s3.secret-access-key", TestConfig.getMinioSecretKey());
    options.put("s3.path-style-access", "true");
    options.put("client.region", "us-west-2");
    options.put("warehouse-path", TestConfig.getMinioWarehousePath());
    return options;
  }

  @Test
  void testStorageManagerCreation() throws Exception {
    Map<String, String> options = createTestOptions();
    StorageManager manager = new StorageManager(options);

    assertNotNull(manager);
    assertFalse(manager.isClosed());
    assertNotNull(manager.getFileIO());
    assertEquals(TestConfig.getMinioWarehousePath(), manager.getWarehousePath());
    manager.close();
  }

  @Test
  void testStorageManagerNullOptions() {
    assertThrows(IllegalArgumentException.class, () -> new StorageManager(null));
  }

  @Test
  void testStorageManagerMissingWarehousePath() {
    Map<String, String> options = createTestOptions();
    options.remove("warehouse-path");
    assertThrows(IllegalArgumentException.class, () -> new StorageManager(options));
  }

  @Test
  void testStorageManagerNullWarehousePath() {
    Map<String, String> options = createTestOptions();
    options.put("warehouse-path", null);
    assertThrows(IllegalArgumentException.class, () -> new StorageManager(options));
  }

  @Test
  void testStorageManagerEmptyWarehousePath() {
    Map<String, String> options = createTestOptions();
    options.put("warehouse-path", "");
    assertThrows(IllegalArgumentException.class, () -> new StorageManager(options));
  }

  @Test
  void testStorageManagerInvalidIOImpl() {
    Map<String, String> options = createTestOptions();
    options.put("io-impl", "com.example.NonExistentFileIO");
    assertThrows(IllegalArgumentException.class, () -> new StorageManager(options));
  }

  @Test
  void testStorageManagerNonFileIOClass() {
    Map<String, String> options = createTestOptions();
    // Use a class that exists but doesn't implement FileIO
    options.put("io-impl", "java.lang.String");
    assertThrows(IllegalArgumentException.class, () -> new StorageManager(options));
  }

  @Test
  void testStorageManagerClose() throws Exception {
    Map<String, String> options = createTestOptions();
    StorageManager manager = new StorageManager(options);

    assertFalse(manager.isClosed());
    manager.close();
    assertTrue(manager.isClosed());
  }

  @Test
  void testStorageManagerCloseIdempotent() throws Exception {
    Map<String, String> options = createTestOptions();
    StorageManager manager = new StorageManager(options);

    manager.close();
    assertTrue(manager.isClosed());
    // Calling close again should be safe
    manager.close();
    assertTrue(manager.isClosed());
  }

  @Test
  void testStorageManagerGetFileIOWhenClosed() throws Exception {
    Map<String, String> options = createTestOptions();
    StorageManager manager = new StorageManager(options);
    manager.close();

    assertThrows(IllegalStateException.class, manager::getFileIO);
  }

  @Test
  void testStorageManagerGetWarehousePathWhenClosed() throws Exception {
    Map<String, String> options = createTestOptions();
    StorageManager manager = new StorageManager(options);
    manager.close();

    assertThrows(IllegalStateException.class, manager::getWarehousePath);
  }

  @Test
  void testStorageManagerIsHealthyWhenClosed() throws Exception {
    Map<String, String> options = createTestOptions();
    StorageManager manager = new StorageManager(options);
    manager.close();

    assertFalse(manager.isHealthy());
  }

  @Test
  void testStorageManagerIsHealthy() throws Exception {
    Map<String, String> options = createTestOptions();
    StorageManager manager = new StorageManager(options);

    // With Docker Compose running, test the actual health check against MinIO
    // This executes fileIO.newInputFile() to verify connectivity
    assertTrue(manager.isHealthy(), "Storage should be healthy when MinIO is running");
    manager.close();
  }
}
