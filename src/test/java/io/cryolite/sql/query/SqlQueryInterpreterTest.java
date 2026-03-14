package io.cryolite.sql.query;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.cryolite.CryoliteEngine;
import io.cryolite.sql.SqlExecutionException;
import io.cryolite.sql.SqlSession;
import java.io.IOException;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SqlQueryInterpreter} error paths.
 *
 * <p>Happy-path SELECT execution is covered by {@code SqlSelectIntegrationTest}. These tests verify
 * error handling and edge cases using a mocked engine, without requiring Docker infrastructure.
 */
class SqlQueryInterpreterTest {

  private CryoliteEngine mockEngine() {
    CryoliteEngine engine = mock(CryoliteEngine.class);
    when(engine.getCatalog()).thenReturn(mock(Catalog.class));
    return engine;
  }

  @Test
  void selectWithoutFromClauseThrowsSqlExecutionException() {
    CryoliteEngine engine = mockEngine();

    try (SqlSession session = new SqlSession(engine)) {
      SqlExecutionException ex =
          assertThrows(SqlExecutionException.class, () -> session.query("SELECT 1"));
      assertTrue(
          ex.getMessage().contains("FROM"),
          "Error should mention FROM clause, was: " + ex.getMessage());
    }
  }

  @Test
  void selectFromUnqualifiedTableThrowsSqlExecutionException() {
    CryoliteEngine engine = mockEngine();

    try (SqlSession session = new SqlSession(engine)) {
      SqlExecutionException ex =
          assertThrows(
              SqlExecutionException.class, () -> session.query("SELECT * FROM unqualified_table"));
      assertTrue(
          ex.getMessage().contains("fully qualified"),
          "Error should mention fully qualified, was: " + ex.getMessage());
    }
  }

  @Test
  void ioExceptionDuringScanIsWrappedInSqlExecutionException() throws Exception {
    CryoliteEngine engine = mockEngine();
    when(engine.scan(any(TableIdentifier.class))).thenThrow(new IOException("connection lost"));

    try (SqlSession session = new SqlSession(engine)) {
      SqlExecutionException ex =
          assertThrows(
              SqlExecutionException.class, () -> session.query("SELECT * FROM test_ns.test_table"));
      assertTrue(
          ex.getMessage().contains("Failed to scan table"),
          "Error should mention scan failure, was: " + ex.getMessage());
      assertInstanceOf(IOException.class, ex.getCause());
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  void selectStarReturnsEngineResult() throws Exception {
    CryoliteEngine engine = mockEngine();
    CloseableIterable<VectorSchemaRoot> expectedBatches = mock(CloseableIterable.class);
    when(engine.scan(any(TableIdentifier.class))).thenReturn(expectedBatches);

    try (SqlSession session = new SqlSession(engine)) {
      CloseableIterable<VectorSchemaRoot> result =
          session.query("SELECT * FROM test_ns.test_table");
      assertSame(expectedBatches, result, "query() must return the engine's scan result");
    }

    verify(engine).scan(TableIdentifier.of("test_ns", "test_table"));
  }
}
