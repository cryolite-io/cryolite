package io.cryolite.sql;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

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
    // A mock catalog suffices; these tests do not reach catalog execution.
    return new SqlSession(mock(Catalog.class));
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
  void executeUnsupportedStatementTypeThrowsSqlExecutionException() {
    // SELECT is valid SQL but not yet supported in M5.
    try (SqlSession session = session()) {
      SqlExecutionException ex =
          assertThrows(
              SqlExecutionException.class, () -> session.execute("SELECT 1 FROM my_table"));
      assertTrue(
          ex.getMessage().contains("Unsupported SQL statement type"),
          "Error message should mention unsupported type, was: " + ex.getMessage());
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
