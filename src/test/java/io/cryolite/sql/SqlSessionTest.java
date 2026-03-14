package io.cryolite.sql;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.cryolite.CryoliteEngine;
import org.apache.iceberg.catalog.Catalog;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SqlSession}.
 *
 * <p>These tests verify the SQL parsing and dispatch logic in isolation, without requiring any
 * catalog or Docker infrastructure. Catalog interaction is covered by {@link
 * SqlDdlIntegrationTest}.
 */
class SqlSessionTest {

  private SqlSession session() {
    // Mock the engine; these tests verify parsing/dispatch only and never reach catalog execution.
    CryoliteEngine engine = mock(CryoliteEngine.class);
    when(engine.getCatalog()).thenReturn(mock(Catalog.class));
    return new SqlSession(engine);
  }

  @Test
  void executeNullSqlThrowsSqlExecutionException() {
    try (SqlSession session = session()) {
      SqlExecutionException ex =
          assertThrows(SqlExecutionException.class, () -> session.execute(null));
      assertTrue(ex.getMessage().contains("null or blank"));
    }
  }

  @Test
  void executeBlankSqlThrowsSqlExecutionException() {
    try (SqlSession session = session()) {
      SqlExecutionException ex =
          assertThrows(SqlExecutionException.class, () -> session.execute("   "));
      assertTrue(ex.getMessage().contains("null or blank"));
    }
  }

  @Test
  void executeSelectViaExecuteThrowsWithHelpfulMessage() {
    // SELECT must use query(), not execute().
    try (SqlSession session = session()) {
      SqlExecutionException ex =
          assertThrows(
              SqlExecutionException.class, () -> session.execute("SELECT 1 FROM my_table"));
      assertTrue(
          ex.getMessage().contains("query()"),
          "Error message should suggest query(), was: " + ex.getMessage());
    }
  }

  @Test
  void executeUnsupportedStatementTypeThrowsSqlExecutionException() {
    // DELETE is valid SQL but not supported.
    try (SqlSession session = session()) {
      SqlExecutionException ex =
          assertThrows(
              SqlExecutionException.class,
              () -> session.execute("DELETE FROM my_ns.my_table WHERE id = 1"));
      assertTrue(
          ex.getMessage().contains("Unsupported SQL statement type"),
          "Error message should mention unsupported type, was: " + ex.getMessage());
    }
  }

  @Test
  void queryNullSqlThrowsSqlExecutionException() {
    try (SqlSession session = session()) {
      SqlExecutionException ex =
          assertThrows(SqlExecutionException.class, () -> session.query(null));
      assertTrue(ex.getMessage().contains("null or blank"));
    }
  }

  @Test
  void queryNonSelectThrowsSqlExecutionException() {
    try (SqlSession session = session()) {
      SqlExecutionException ex =
          assertThrows(
              SqlExecutionException.class, () -> session.query("CREATE TABLE ns.t (id BIGINT)"));
      assertTrue(
          ex.getMessage().contains("SELECT"),
          "Error message should mention SELECT, was: " + ex.getMessage());
    }
  }

  @Test
  void executeInvalidSqlThrowsSqlExecutionException() {
    try (SqlSession session = session()) {
      SqlExecutionException ex =
          assertThrows(SqlExecutionException.class, () -> session.execute("NOT VALID SQL !!!"));
      assertTrue(
          ex.getMessage().contains("Failed to parse SQL"),
          "Error message should mention parse failure, was: " + ex.getMessage());
    }
  }

  @Test
  void closeIsIdempotentAndDoesNotThrow() {
    // Verifies AutoCloseable contract: close() must not throw.
    SqlSession session = session();
    assertDoesNotThrow(session::close);
    assertDoesNotThrow(session::close); // idempotent
  }

  @Test
  void sqlExecutionExceptionPreservesCause() {
    Throwable cause = new RuntimeException("root cause");
    SqlExecutionException ex = new SqlExecutionException("wrapped", cause);
    assertEquals("wrapped", ex.getMessage());
    assertSame(cause, ex.getCause());
  }
}
