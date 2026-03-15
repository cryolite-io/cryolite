package io.cryolite.filter;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ComparisonOperator}.
 *
 * <p>Verifies all six SQL comparison operators with same-type operands, cross-type numeric
 * promotion, and Arrow Text handling.
 */
class ComparisonOperatorTest {

  // --- EQUALS ---

  @Test
  void equalsReturnsTrueForEqualValues() {
    assertTrue(ComparisonOperator.EQUALS.apply(42L, 42L));
  }

  @Test
  void equalsReturnsFalseForDifferentValues() {
    assertFalse(ComparisonOperator.EQUALS.apply(42L, 99L));
  }

  // --- NOT_EQUALS ---

  @Test
  void notEqualsReturnsTrueForDifferentValues() {
    assertTrue(ComparisonOperator.NOT_EQUALS.apply(10L, 20L));
  }

  @Test
  void notEqualsReturnsFalseForEqualValues() {
    assertFalse(ComparisonOperator.NOT_EQUALS.apply(10L, 10L));
  }

  // --- LESS_THAN ---

  @Test
  void lessThanReturnsTrueWhenColumnIsSmaller() {
    assertTrue(ComparisonOperator.LESS_THAN.apply(5L, 10L));
  }

  @Test
  void lessThanReturnsFalseWhenEqual() {
    assertFalse(ComparisonOperator.LESS_THAN.apply(10L, 10L));
  }

  @Test
  void lessThanReturnsFalseWhenColumnIsLarger() {
    assertFalse(ComparisonOperator.LESS_THAN.apply(15L, 10L));
  }

  // --- LESS_THAN_OR_EQUAL ---

  @Test
  void lessThanOrEqualReturnsTrueWhenEqual() {
    assertTrue(ComparisonOperator.LESS_THAN_OR_EQUAL.apply(10L, 10L));
  }

  @Test
  void lessThanOrEqualReturnsTrueWhenSmaller() {
    assertTrue(ComparisonOperator.LESS_THAN_OR_EQUAL.apply(5L, 10L));
  }

  @Test
  void lessThanOrEqualReturnsFalseWhenLarger() {
    assertFalse(ComparisonOperator.LESS_THAN_OR_EQUAL.apply(15L, 10L));
  }

  // --- GREATER_THAN ---

  @Test
  void greaterThanReturnsTrueWhenColumnIsLarger() {
    assertTrue(ComparisonOperator.GREATER_THAN.apply(15L, 10L));
  }

  @Test
  void greaterThanReturnsFalseWhenEqual() {
    assertFalse(ComparisonOperator.GREATER_THAN.apply(10L, 10L));
  }

  // --- GREATER_THAN_OR_EQUAL ---

  @Test
  void greaterThanOrEqualReturnsTrueWhenEqual() {
    assertTrue(ComparisonOperator.GREATER_THAN_OR_EQUAL.apply(10L, 10L));
  }

  @Test
  void greaterThanOrEqualReturnsTrueWhenLarger() {
    assertTrue(ComparisonOperator.GREATER_THAN_OR_EQUAL.apply(15L, 10L));
  }

  @Test
  void greaterThanOrEqualReturnsFalseWhenSmaller() {
    assertFalse(ComparisonOperator.GREATER_THAN_OR_EQUAL.apply(5L, 10L));
  }

  // --- Numeric promotion (Integer vs Long) ---

  @Test
  void equalsWithIntegerAndLongPromotion() {
    assertTrue(ComparisonOperator.EQUALS.apply(42, 42L));
    assertTrue(ComparisonOperator.EQUALS.apply(42L, 42));
  }

  @Test
  void lessThanWithMixedNumericTypes() {
    assertTrue(ComparisonOperator.LESS_THAN.apply(5, 10L));
    assertFalse(ComparisonOperator.LESS_THAN.apply(10.0, 5L));
  }

  // --- Arrow Text handling ---

  @Test
  void equalsWithArrowTextAndString() {
    Text arrowText = new Text("hello");
    assertTrue(ComparisonOperator.EQUALS.apply(arrowText, "hello"));
    assertFalse(ComparisonOperator.EQUALS.apply(arrowText, "world"));
  }

  @Test
  void lessThanWithArrowTextComparison() {
    Text arrowText = new Text("apple");
    assertTrue(ComparisonOperator.LESS_THAN.apply(arrowText, "banana"));
    assertFalse(ComparisonOperator.LESS_THAN.apply(arrowText, "aardvark"));
  }

  // --- String comparison ---

  @Test
  void equalsWithStringValues() {
    assertTrue(ComparisonOperator.EQUALS.apply("abc", "abc"));
    assertFalse(ComparisonOperator.EQUALS.apply("abc", "xyz"));
  }
}
