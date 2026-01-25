package io.cryolite;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Tests for CryoliteEngine. */
class CryoliteEngineTest {

  private CryoliteConfig createTestConfig() {
    return new CryoliteConfig.Builder()
        .catalogOption("uri", TestConfig.getPolarisUri())
        .catalogOption(
            "credential",
            TestConfig.getPolarisClientId() + ":" + TestConfig.getPolarisClientSecret())
        .catalogOption("scope", TestConfig.getPolarisScope())
        .catalogOption("warehouse", TestConfig.getPolarisWarehouse())
        .storageOption("endpoint", TestConfig.getMinioEndpoint())
        .storageOption("access-key", TestConfig.getMinioAccessKey())
        .storageOption("secret-key", TestConfig.getMinioSecretKey())
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
}
