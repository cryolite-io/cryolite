package io.cryolite.sql;

/**
 * Exception thrown when SQL execution fails.
 *
 * <p>This exception wraps errors that occur during SQL parsing, validation, or execution within the
 * CRYOLITE SQL layer. It provides a clear, documented exception type for all SQL-layer failures.
 *
 * @since 0.1.0
 */
public class SqlExecutionException extends RuntimeException {

  /**
   * Creates a new SqlExecutionException with the given message.
   *
   * @param message the error message
   */
  public SqlExecutionException(String message) {
    super(message);
  }

  /**
   * Creates a new SqlExecutionException with the given message and cause.
   *
   * @param message the error message
   * @param cause the underlying cause
   */
  public SqlExecutionException(String message, Throwable cause) {
    super(message, cause);
  }
}
