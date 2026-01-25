package io.cryolite;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Tests for CryoliteEngine. */
class CryoliteEngineTest {

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

  private CryoliteConfig createTestConfig() {
    return new CryoliteConfig.Builder()
        .catalogOption("uri", TestConfig.getPolarisUri())
        .catalogOption(
            "credential",
            TestConfig.getPolarisClientId() + ":" + TestConfig.getPolarisClientSecret())
        .catalogOption("scope", TestConfig.getPolarisScope())
        .catalogOption("warehouse", TestConfig.getPolarisWarehouse())
        .storageOption("io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .storageOption("s3.endpoint", TestConfig.getMinioEndpoint())
        .storageOption("s3.access-key-id", TestConfig.getMinioAccessKey())
        .storageOption("s3.secret-access-key", TestConfig.getMinioSecretKey())
        .storageOption("s3.path-style-access", "true")
        .storageOption("client.region", "us-west-2")
        .storageOption("warehouse-path", TestConfig.getMinioWarehousePath())
        .build();
  }

  @Test
  void testEngineCreation() throws Exception {
    CryoliteConfig config = createTestConfig();
    CryoliteEngine engine = new CryoliteEngine(config);

    assertNotNull(engine);
    assertFalse(engine.isClosed());
    assertEquals(config, engine.getConfig());
    engine.close();
  }

  @Test
  void testEngineClose() throws Exception {
    CryoliteConfig config = createTestConfig();
    CryoliteEngine engine = new CryoliteEngine(config);

    assertFalse(engine.isClosed());
    engine.close();
    assertTrue(engine.isClosed());
  }

  @Test
  void testEngineCloseIdempotent() throws Exception {
    CryoliteConfig config = createTestConfig();
    CryoliteEngine engine = new CryoliteEngine(config);

    engine.close();
    assertTrue(engine.isClosed());
    // Calling close again should be safe (idempotent)
    engine.close();
    assertTrue(engine.isClosed());
  }

  @Test
  void testEngineNullConfig() {
    assertThrows(IllegalArgumentException.class, () -> new CryoliteEngine(null));
  }

  @Test
  void testEngineToString() throws Exception {
    CryoliteConfig config = createTestConfig();
    CryoliteEngine engine = new CryoliteEngine(config);

    String str = engine.toString();
    assertNotNull(str);
    assertTrue(str.contains("CryoliteEngine"));
    engine.close();
  }

  @Test
  void testGetCatalogManager() throws Exception {
    CryoliteConfig config = createTestConfig();
    CryoliteEngine engine = new CryoliteEngine(config);

    assertNotNull(engine.getCatalogManager());
    engine.close();
  }

  @Test
  void testGetCatalogManagerWhenClosed() throws Exception {
    CryoliteConfig config = createTestConfig();
    CryoliteEngine engine = new CryoliteEngine(config);
    engine.close();

    assertThrows(IllegalStateException.class, engine::getCatalogManager);
  }

  @Test
  void testGetStorageManager() throws Exception {
    CryoliteConfig config = createTestConfig();
    CryoliteEngine engine = new CryoliteEngine(config);

    assertNotNull(engine.getStorageManager());
    engine.close();
  }

  @Test
  void testGetStorageManagerWhenClosed() throws Exception {
    CryoliteConfig config = createTestConfig();
    CryoliteEngine engine = new CryoliteEngine(config);
    engine.close();

    assertThrows(IllegalStateException.class, engine::getStorageManager);
  }

  @Test
  void testIsHealthy() throws Exception {
    CryoliteConfig config = createTestConfig();
    CryoliteEngine engine = new CryoliteEngine(config);

    // With Docker Compose running, the engine should be healthy
    // This tests the actual health check logic against real services
    assertTrue(engine.isHealthy(), "Engine should be healthy when services are running");
    engine.close();
  }

  @Test
  void testIsHealthyWhenClosed() throws Exception {
    CryoliteConfig config = createTestConfig();
    CryoliteEngine engine = new CryoliteEngine(config);
    engine.close();

    assertFalse(engine.isHealthy());
  }
}
