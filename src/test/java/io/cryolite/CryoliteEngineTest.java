package io.cryolite;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Tests for CryoliteEngine. */
class CryoliteEngineTest {

  @Test
  void testEngineCreation() {
    CryoliteConfig config = new CryoliteConfig.Builder().build();
    CryoliteEngine engine = new CryoliteEngine(config);

    assertNotNull(engine);
    assertFalse(engine.isClosed());
    assertEquals(config, engine.getConfig());
  }

  @Test
  void testEngineClose() {
    CryoliteConfig config = new CryoliteConfig.Builder().build();
    CryoliteEngine engine = new CryoliteEngine(config);

    assertFalse(engine.isClosed());
    engine.close();
    assertTrue(engine.isClosed());
  }

  @Test
  void testEngineCloseIdempotent() {
    CryoliteConfig config = new CryoliteConfig.Builder().build();
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
  void testEngineToString() {
    CryoliteConfig config = new CryoliteConfig.Builder().build();
    CryoliteEngine engine = new CryoliteEngine(config);

    String str = engine.toString();
    assertNotNull(str);
    assertTrue(str.contains("CryoliteEngine"));
  }
}
