package io.cryolite.sql;

import io.cryolite.CryoliteEngine;
import io.cryolite.sql.ddl.SqlDdlInterpreter;
import io.cryolite.sql.dml.SqlDmlInterpreter;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;

/**
 * Entry point for executing SQL statements against CRYOLITE.
 *
 * <p>A {@code SqlSession} is the high-level SQL API of CRYOLITE. It uses Apache Calcite to parse
 * SQL strings and dispatches the resulting AST nodes to the appropriate interpreter (DDL, DML,
 * query).
 *
 * <p>Create a session via {@code CryoliteEngine.createSqlSession()} and execute SQL statements with
 * {@link #execute(String)}.
 *
 * <p>Supported SQL:
 *
 * <ul>
 *   <li>{@code CREATE TABLE [IF NOT EXISTS] namespace.table (columns...)} (M5)
 *   <li>{@code INSERT INTO namespace.table [(cols)] VALUES (vals) [, (vals)]} (M6)
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * try (SqlSession session = engine.createSqlSession()) {
 *   session.execute(
 *     "CREATE TABLE analytics.events (" +
 *     "  event_id  BIGINT     NOT NULL," +
 *     "  user_id   BIGINT," +
 *     "  event_ts  TIMESTAMP  NOT NULL," +
 *     "  payload   VARCHAR" +
 *     ")"
 *   );
 * }
 * }</pre>
 *
 * @since 0.1.0
 */
public class SqlSession implements AutoCloseable {

  private final SqlDdlInterpreter ddlInterpreter;
  private final SqlDmlInterpreter dmlInterpreter;

  /**
   * Creates a new SqlSession backed by the given engine.
   *
   * <p>Prefer using {@code CryoliteEngine.createSqlSession()} rather than calling this constructor
   * directly, as the engine ensures the session is created in a valid, open state.
   *
   * @param engine the CRYOLITE engine used for DDL and DML execution
   */
  public SqlSession(CryoliteEngine engine) {
    this.ddlInterpreter = new SqlDdlInterpreter(engine.getCatalog());
    this.dmlInterpreter = new SqlDmlInterpreter(engine);
  }

  /**
   * Parses and executes a SQL statement.
   *
   * <p>Supported statement types: {@code CREATE TABLE} (DDL) and {@code INSERT INTO} (DML).
   *
   * @param sql the SQL string to execute
   * @throws SqlExecutionException if the SQL cannot be parsed, is unsupported, or execution fails
   */
  public void execute(String sql) {
    if (sql == null || sql.isBlank()) {
      throw new SqlExecutionException("SQL statement must not be null or blank");
    }

    SqlNode parsed = parse(sql);
    dispatch(parsed, sql);
  }

  private SqlNode parse(String sql) {
    // Use the DDL-aware parser from calcite-server so that CREATE TABLE
    // and other DDL statements are recognized in addition to standard SQL.
    // Preserve the original identifier casing (UNCHANGED) so that table/namespace
    // names like 'my_namespace.my_table' are not silently uppercased.
    SqlParser.Config ddlConfig =
        SqlParser.config()
            .withParserFactory(SqlDdlParserImpl.FACTORY)
            .withUnquotedCasing(Casing.UNCHANGED)
            .withQuotedCasing(Casing.UNCHANGED);
    SqlParser parser = SqlParser.create(sql, ddlConfig);
    try {
      return parser.parseStmt();
    } catch (SqlParseException e) {
      throw new SqlExecutionException("Failed to parse SQL: " + e.getMessage(), e);
    }
  }

  private void dispatch(SqlNode node, String sql) {
    if (node instanceof SqlCreateTable createTable) {
      ddlInterpreter.execute(createTable);
      return;
    }
    if (node instanceof SqlInsert insert) {
      dmlInterpreter.execute(insert);
      return;
    }
    throw new SqlExecutionException(
        "Unsupported SQL statement type: '"
            + node.getKind()
            + "'. "
            + "Supported statements: CREATE TABLE, INSERT INTO. Statement: "
            + sql);
  }

  /**
   * Closes the session. Currently a no-op; reserved for future resource management.
   *
   * <p>Implementing {@link AutoCloseable} allows use in try-with-resources blocks, which is the
   * recommended usage pattern.
   */
  @Override
  public void close() {
    // No resources to release in this version. Reserved for future connection/transaction mgmt.
  }
}
