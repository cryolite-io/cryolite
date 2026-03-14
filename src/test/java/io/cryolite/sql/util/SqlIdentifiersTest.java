package io.cryolite.sql.util;

import static org.junit.jupiter.api.Assertions.*;

import io.cryolite.sql.SqlExecutionException;
import java.util.List;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SqlIdentifiers}.
 *
 * <p>These tests verify the mapping from Calcite SQL identifiers to Iceberg table identifiers,
 * including validation of fully-qualified names.
 */
class SqlIdentifiersTest {

  @Test
  void resolvesTwoPartIdentifier() {
    SqlIdentifier id = new SqlIdentifier(List.of("my_ns", "my_table"), SqlParserPos.ZERO);
    TableIdentifier result = SqlIdentifiers.resolveTableIdentifier(id);

    assertEquals("my_ns", result.namespace().toString());
    assertEquals("my_table", result.name());
  }

  @Test
  void resolvesThreePartIdentifierUsesLastTwoParts() {
    SqlIdentifier id =
        new SqlIdentifier(List.of("catalog", "my_ns", "my_table"), SqlParserPos.ZERO);
    TableIdentifier result = SqlIdentifiers.resolveTableIdentifier(id);

    assertEquals("my_ns", result.namespace().toString());
    assertEquals("my_table", result.name());
  }

  @Test
  void singlePartIdentifierThrowsSqlExecutionException() {
    SqlIdentifier id = new SqlIdentifier(List.of("only_table"), SqlParserPos.ZERO);

    SqlExecutionException ex =
        assertThrows(SqlExecutionException.class, () -> SqlIdentifiers.resolveTableIdentifier(id));
    assertTrue(
        ex.getMessage().contains("fully qualified"),
        "Error message should mention 'fully qualified', was: " + ex.getMessage());
    assertTrue(
        ex.getMessage().contains("only_table"),
        "Error message should contain the identifier, was: " + ex.getMessage());
  }

  @Test
  void preservesCaseSensitiveNames() {
    SqlIdentifier id = new SqlIdentifier(List.of("MyNamespace", "MyTable"), SqlParserPos.ZERO);
    TableIdentifier result = SqlIdentifiers.resolveTableIdentifier(id);

    assertEquals("MyNamespace", result.namespace().toString());
    assertEquals("MyTable", result.name());
  }
}
