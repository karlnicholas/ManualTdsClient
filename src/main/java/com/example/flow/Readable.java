package com.example.flow;

import io.r2dbc.spi.OutParameters;

import java.util.NoSuchElementException;

/**
 * Represents a readable object, for example a set of columns or {@code OUT} parameters from a database query, later on referred to as items.
 * Values can for columns or {@code OUT} parameters be either retrieved by specifying a name or the index.
 * Indexes are {@code 0}-based.
 *
 * <p> Column and {@code OUT} parameter names used as input to getter methods are case-insensitive.
 * When a {@code get} method is called with a name and several items have the same name, then the value of the first matching item will be returned.
 * Items that are not explicitly named in the query should be referenced through indexes.
 *
 * <p>For maximum portability, items within each {@link io.r2dbc.spi.Readable} should be read in left-to-right order, and each item should be read only once.
 *
 * <p>{@link #get(String)} and {@link #get(int)} without specifying a target type returns a suitable value representation.  The R2DBC specification contains a mapping table that shows default
 * mappings between database types and Java types.
 * Specifying a target type, the R2DBC driver attempts to convert the value to the target type.
 * <p>A item is invalidated after consumption.
 *
 * @see OutParameters
 * @since 0.9
 */
public interface Readable {

  /**
   * Returns the value using the default type mapping.  The default implementation of this method calls {@link #get(int, Class)} passing {@link Object} as the type
   * to allow the implementation to make the loosest possible match.
   *
   * @param index the index of the parameter starting at {@code 0}
   * @return the value.  Value can be {@code null}.
   * @throws IndexOutOfBoundsException if {@code index} if the index is out of range (negative or equals/exceeds the number of readable objects)
   */
  default Object get(int index) {
    return get(index, Object.class);
  }

  /**
   * Returns the value for a parameter.  Use {@link Object} to allow the implementation to make the loosest possible match.
   *
   * @param index the index starting at {@code 0}
   * @param type  the type of item to return.  This type must be assignable to, and allows for variance.
   * @param <T>   the type of the item being returned.
   * @return the value.  Value can be {@code null}.
   * @throws IllegalArgumentException  if {@code type} is {@code null}
   * @throws IndexOutOfBoundsException if {@code index} is out of range (negative or equals/exceeds the number of readable objects)
   */
  <T> T get(int index, Class<T> type);

  /**
   * Returns the value for a parameter using the default type mapping.  The default implementation of this method calls {@link #get(String, Class)} passing {@link Object} as the type in
   * order to allow the implementation to make the loosest possible match.
   *
   * @param name the name
   * @return the value.  Value can be {@code null}.
   * @throws IllegalArgumentException if {@code name} is {@code null}
   * @throws NoSuchElementException   if {@code name} is not a known readable column or out parameter
   */
  default Object get(String name) {
    return get(name, Object.class);
  }

  /**
   * Returns the value.  Use {@link Object} to allow the implementation to make the loosest possible match.
   *
   * @param name the name
   * @param type the type of item to return.  This type must be assignable to, and allows for variance.
   * @param <T>  the type of the item being returned.
   * @return the value.  Value can be {@code null}.
   * @throws IllegalArgumentException if {@code name} or {@code type} is {@code null}
   * @throws NoSuchElementException if {@code name} is not a known readable column or out parameter
   */
  <T> T get(String name, Class<T> type);

}
