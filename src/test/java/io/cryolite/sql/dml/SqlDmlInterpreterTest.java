package io.cryolite.sql.dml;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.cryolite.CryoliteEngine;
import io.cryolite.sql.SqlExecutionException;
import io.cryolite.sql.SqlSession;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SqlDmlInterpreter} error paths.
 *
 * <p>Happy-path INSERT execution is covered by {@code SqlDmlIntegrationTest}. These tests verify
 * error handling and edge cases using a mocked engine, without requiring Docker infrastructure.
 */
class SqlDmlInterpreterTest {

  /**
   * Creates a mock engine with a catalog that contains a table with the given schema. The table is
   * registered under {@code test_ns.test_table}.
   */
  private CryoliteEngine engineWithTable(Schema schema) {
    CryoliteEngine engine = mock(CryoliteEngine.class);
    Catalog catalog = mock(Catalog.class);
    Table table = mock(Table.class);

    when(engine.getCatalog()).thenReturn(catalog);
    when(catalog.tableExists(any(TableIdentifier.class))).thenReturn(true);
    when(catalog.loadTable(any(TableIdentifier.class))).thenReturn(table);
    when(table.schema()).thenReturn(schema);

    return engine;
  }

  /**
   * Helper: executes a SQL INSERT via a real SqlSession backed by the given engine. This exercises
   * the full parse → dispatch → interpreter path.
   */
  private void executeInsert(CryoliteEngine engine, String sql) {
    try (SqlSession session = new SqlSession(engine)) {
      session.execute(sql);
    }
  }

  @Test
  void columnCountMismatchThrowsSqlExecutionException() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    CryoliteEngine engine = engineWithTable(schema);

    // INSERT specifies 2 columns implicitly (schema has 2), but VALUES has 3 values
    SqlExecutionException ex =
        assertThrows(
            SqlExecutionException.class,
            () -> executeInsert(engine, "INSERT INTO test_ns.test_table VALUES (1, 'a', 99)"));
    assertTrue(
        ex.getMessage().contains("Column count mismatch"),
        "Error should mention column count mismatch, was: " + ex.getMessage());
  }

  @Test
  void insertIntoTableWithUnknownColumnThrowsSqlExecutionException() {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

    CryoliteEngine engine = engineWithTable(schema);

    // Explicit column list references a column that does not exist in the schema
    SqlExecutionException ex =
        assertThrows(
            SqlExecutionException.class,
            () -> executeInsert(engine, "INSERT INTO test_ns.test_table (no_such_col) VALUES (1)"));
    assertTrue(
        ex.getMessage().contains("Column not found"),
        "Error should mention column not found, was: " + ex.getMessage());
  }

  @Test
  void ioExceptionDuringAppendIsWrappedInSqlExecutionException() throws Exception {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

    CryoliteEngine engine = engineWithTable(schema);
    doThrow(new java.io.IOException("disk full"))
        .when(engine)
        .append(any(TableIdentifier.class), anyList());

    SqlExecutionException ex =
        assertThrows(
            SqlExecutionException.class,
            () -> executeInsert(engine, "INSERT INTO test_ns.test_table VALUES (1)"));
    assertTrue(
        ex.getMessage().contains("Failed to commit INSERT"),
        "Error should mention commit failure, was: " + ex.getMessage());
    assertInstanceOf(java.io.IOException.class, ex.getCause());
  }
}
