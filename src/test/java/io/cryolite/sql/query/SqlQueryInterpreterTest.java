package io.cryolite.sql.query;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.cryolite.CryoliteEngine;
import io.cryolite.filter.BatchPredicate;
import io.cryolite.sql.SqlExecutionException;
import io.cryolite.sql.SqlSession;
import java.io.IOException;
import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
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

  @Test
  void ioExceptionDuringFilteredScanIsWrappedInSqlExecutionException() throws Exception {
    CryoliteEngine engine = mockEngine();
    Table table = mock(Table.class);
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));
    when(table.schema()).thenReturn(schema);

    Catalog catalog = engine.getCatalog();
    when(catalog.tableExists(any(TableIdentifier.class))).thenReturn(true);
    when(catalog.loadTable(any(TableIdentifier.class))).thenReturn(table);

    when(engine.scan(any(TableIdentifier.class), isNull(), any(BatchPredicate.class)))
        .thenThrow(new IOException("storage offline"));

    try (SqlSession session = new SqlSession(engine)) {
      SqlExecutionException ex =
          assertThrows(
              SqlExecutionException.class,
              () -> session.query("SELECT * FROM test_ns.test_table WHERE id = 1"));
      assertTrue(
          ex.getMessage().contains("Failed to scan table"),
          "Error should mention scan failure, was: " + ex.getMessage());
      assertInstanceOf(IOException.class, ex.getCause());
    }
  }

  @Test
  void selectWhereOnNonExistentTableThrowsSqlExecutionException() {
    CryoliteEngine engine = mockEngine();
    Catalog catalog = engine.getCatalog();
    when(catalog.tableExists(any(TableIdentifier.class))).thenReturn(false);

    try (SqlSession session = new SqlSession(engine)) {
      SqlExecutionException ex =
          assertThrows(
              SqlExecutionException.class,
              () -> session.query("SELECT * FROM test_ns.test_table WHERE id = 1"));
      assertTrue(
          ex.getMessage().contains("Table does not exist"),
          "Error should mention table not found, was: " + ex.getMessage());
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  void selectColumnListPushesProjectionToEngine() throws Exception {
    CryoliteEngine engine = mockEngine();
    CloseableIterable<VectorSchemaRoot> expectedBatches = mock(CloseableIterable.class);
    when(engine.scan(any(TableIdentifier.class), any(List.class), any(BatchPredicate.class)))
        .thenReturn(expectedBatches);

    try (SqlSession session = new SqlSession(engine)) {
      CloseableIterable<VectorSchemaRoot> result =
          session.query("SELECT id, name FROM test_ns.test_table");
      assertSame(expectedBatches, result);
    }

    verify(engine)
        .scan(
            eq(TableIdentifier.of("test_ns", "test_table")),
            eq(List.of("id", "name")),
            any(BatchPredicate.class));
  }

  @SuppressWarnings("unchecked")
  @Test
  void selectColumnListWithWhereUsesThreeArgScan() throws Exception {
    CryoliteEngine engine = mockEngine();
    Table table = mock(Table.class);
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));
    when(table.schema()).thenReturn(schema);

    Catalog catalog = engine.getCatalog();
    when(catalog.tableExists(any(TableIdentifier.class))).thenReturn(true);
    when(catalog.loadTable(any(TableIdentifier.class))).thenReturn(table);

    CloseableIterable<VectorSchemaRoot> expectedBatches = mock(CloseableIterable.class);
    when(engine.scan(any(TableIdentifier.class), any(List.class), any(BatchPredicate.class)))
        .thenReturn(expectedBatches);

    try (SqlSession session = new SqlSession(engine)) {
      CloseableIterable<VectorSchemaRoot> result =
          session.query("SELECT id FROM test_ns.test_table WHERE id = 1");
      assertSame(expectedBatches, result);
    }

    verify(engine)
        .scan(
            eq(TableIdentifier.of("test_ns", "test_table")),
            eq(List.of("id")),
            any(BatchPredicate.class));
  }

  @Test
  void unsupportedSelectListExpressionThrowsSqlExecutionException() {
    CryoliteEngine engine = mockEngine();

    try (SqlSession session = new SqlSession(engine)) {
      // "1 + 1" is a call expression, not a simple identifier – should throw
      SqlExecutionException ex =
          assertThrows(
              SqlExecutionException.class,
              () -> session.query("SELECT 1 + 1 FROM test_ns.test_table"));
      assertTrue(
          ex.getMessage().contains("Unsupported expression in SELECT list"),
          "Unexpected message: " + ex.getMessage());
    }
  }
}
