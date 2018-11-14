package io.rsocket.internal;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.IntFunction;

import static java.util.Objects.requireNonNull;

/**
 * {@link Map} implementation specialised for int keys using open addressing and linear probing for
 * cache efficient access.
 *
 * @param <V> type of values stored in the {@link Map}
 */
public class AtomicInt2ObjectHashMap<V> implements Map<Integer, V>, Serializable {
  private static final float DEFAULT_LOAD_FACTOR = 0.55f;
  private static final int MIN_CAPACITY = 8;

  private final float loadFactor;
  private final boolean shouldAvoidAllocation;
  private int resizeThreshold;
  private int size;
  private AtomicIntegerArray keys;
  private AtomicReferenceArray<V> values;

  private ValueCollection valueCollection;
  private KeySet keySet;
  private EntrySet entrySet;

  public AtomicInt2ObjectHashMap() {
    this(MIN_CAPACITY, DEFAULT_LOAD_FACTOR, true);
  }

  public AtomicInt2ObjectHashMap(final int initialCapacity, final float loadFactor) {
    this(initialCapacity, loadFactor, true);
  }

  /**
   * Construct a new map allowing a configuration for initial capacity and load factor.
   *
   * @param initialCapacity for the backing array
   * @param loadFactor limit for resizing on puts
   * @param shouldAvoidAllocation should allocation be avoided by caching iterators and map entries.
   */
  public AtomicInt2ObjectHashMap(
      final int initialCapacity, final float loadFactor, final boolean shouldAvoidAllocation) {
    validateLoadFactor(loadFactor);

    this.loadFactor = loadFactor;
    this.shouldAvoidAllocation = shouldAvoidAllocation;

    /*  */ final int capacity = findNextPositivePowerOfTwo(Math.max(MIN_CAPACITY, initialCapacity));
    /*  */ resizeThreshold = (int) (capacity * loadFactor);

    keys = new AtomicIntegerArray(capacity);
    values = new AtomicReferenceArray<>(capacity);
  }

  private static int findNextPositivePowerOfTwo(final int value) {
    return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
  }

  /**
   * Validate that a load factor is in the range of 0.1 to 0.9.
   *
   * <p>Load factors in the range 0.5 - 0.7 are recommended for open-addressing with linear probing.
   *
   * @param loadFactor to be validated.
   */
  private static void validateLoadFactor(final float loadFactor) {
    if (loadFactor < 0.1f || loadFactor > 0.9f) {
      throw new IllegalArgumentException(
          "load factor must be in the range of 0.1 to 0.9: " + loadFactor);
    }
  }

  /**
   * Generate a hash for a int value.
   *
   * @param value to be hashed.
   * @param mask mask to be applied that must be a power of 2 - 1.
   * @return the hash of the value.
   */
  private static int hash(final int value, final int mask) {
    final int hash = value * 31;

    return hash & mask;
  }

  /**
   * Get the load factor beyond which the map will increase size.
   *
   * @return load factor for when the map should increase size.
   */
  public float loadFactor() {
    return loadFactor;
  }

  /**
   * Get the total capacity for the map to which the load factor will be a fraction of.
   *
   * @return the total capacity for the map.
   */
  public int capacity() {
    return values.length();
  }

  /**
   * Get the actual threshold which when reached the map will resize. This is a function of the
   * current capacity and load factor.
   *
   * @return the threshold when the map will resize.
   */
  public int resizeThreshold() {
    return resizeThreshold;
  }

  /** {@inheritDoc} */
  public int size() {
    return size;
  }

  /** {@inheritDoc} */
  public boolean isEmpty() {
    return 0 == size;
  }

  /** {@inheritDoc} */
  public boolean containsKey(final Object key) {
    return containsKey(((Integer) key).intValue());
  }

  /**
   * Overloaded version of {@link Map#containsKey(Object)} that takes a primitive int key.
   *
   * @param key for indexing the {@link Map}
   * @return true if the key is found otherwise false.
   */
  public boolean containsKey(final int key) {
    final int mask = values.length() - 1;
    int index = hash(key, mask);

    boolean found = false;
    while (null != values.get(index)) {
      if (key == keys.get(index)) {
        found = true;
        break;
      }

      index = ++index & mask;
    }

    return found;
  }

  /** {@inheritDoc} */
  public boolean containsValue(final Object value) {
    boolean found = false;
    final Object val = mapNullValue(value);
    if (null != val) {
      for (int i = 0; i < values.length(); i++) {
        Object v = values.get(i);
        if (val.equals(v)) {
          found = true;
          break;
        }
      }
    }

    return found;
  }

  /** {@inheritDoc} */
  public V get(final Object key) {
    return get(((Integer) key).intValue());
  }

  /**
   * Overloaded version of {@link Map#get(Object)} that takes a primitive int key.
   *
   * @param key for indexing the {@link Map}
   * @return the value if found otherwise null
   */
  public V get(final int key) {
    return unmapNullValue(getMapped(key));
  }

  @SuppressWarnings("unchecked")
  protected V getMapped(final int key) {
    final int mask = values.length() - 1;
    int index = hash(key, mask);

    Object value;
    while (null != (value = values.get(index))) {
      if (key == keys.get(index)) {
        break;
      }

      index = ++index & mask;
    }

    return (V) value;
  }

  /**
   * Get a value for a given key, or if it does not exist then default the value via a {@link
   * IntFunction} and put it in the map.
   *
   * <p>Primitive specialized version of {@link Map#computeIfAbsent}.
   *
   * @param key to search on.
   * @param mappingFunction to provide a value if the get returns null.
   * @return the value if found otherwise the default.
   */
  public V computeIfAbsent(final int key, final IntFunction<? extends V> mappingFunction) {
    V value = getMapped(key);
    if (value == null) {
      value = mappingFunction.apply(key);
      if (value != null) {
        put(key, value);
      }
    } else {
      value = unmapNullValue(value);
    }

    return value;
  }

  /** {@inheritDoc} */
  public V put(final Integer key, final V value) {
    return put(key.intValue(), value);
  }

  /**
   * Overloaded version of {@link Map#put(Object, Object)} that takes a primitive int key.
   *
   * @param key for indexing the {@link Map}
   * @param value to be inserted in the {@link Map}
   * @return the previous value if found otherwise null
   */
  @SuppressWarnings("unchecked")
  public V put(final int key, final V value) {
    final V val = (V) mapNullValue(value);
    requireNonNull(val, "value cannot be null");

    V oldValue = null;
    final int mask = values.length() - 1;
    int index = hash(key, mask);

    while (null != values.get(index)) {
      if (key == keys.get(index)) {
        oldValue = (V) values.get(index);
        break;
      }

      index = ++index & mask;
    }

    if (null == oldValue) {
      ++size;
      keys.set(index, key);
    }

    values.set(index, val);

    if (size > resizeThreshold) {
      increaseCapacity();
    }

    return unmapNullValue(oldValue);
  }

  /** {@inheritDoc} */
  public V remove(final Object key) {
    return remove(((Integer) key).intValue());
  }

  /**
   * Overloaded version of {@link Map#remove(Object)} that takes a primitive int key.
   *
   * @param key for indexing the {@link Map}
   * @return the value if found otherwise null
   */
  @SuppressWarnings("unchecked")
  public V remove(final int key) {
    final int mask = values.length() - 1;
    int index = hash(key, mask);

    Object value;
    while (null != (value = values.get(index))) {
      if (key == keys.get(index)) {
        values.set(index, null);
        --size;

        compactChain(index);
        break;
      }

      index = ++index & mask;
    }

    return unmapNullValue(value);
  }

  /** {@inheritDoc} */
  public void clear() {
    if (size > 0) {
      values = new AtomicReferenceArray<>(values.length());
      size = 0;
    }
  }

  /**
   * Compact the {@link Map} backing arrays by rehashing with a capacity just larger than current
   * size and giving consideration to the load factor.
   */
  public void compact() {
    final int idealCapacity = (int) Math.round(size() * (1.0d / loadFactor));
    rehash(findNextPositivePowerOfTwo(Math.max(MIN_CAPACITY, idealCapacity)));
  }

  /** {@inheritDoc} */
  public void putAll(final Map<? extends Integer, ? extends V> map) {
    for (final Entry<? extends Integer, ? extends V> entry : map.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  /** {@inheritDoc} */
  public KeySet keySet() {
    if (null == keySet) {
      keySet = new KeySet();
    }

    return keySet;
  }

  /** {@inheritDoc} */
  public ValueCollection values() {
    if (null == valueCollection) {
      valueCollection = new ValueCollection();
    }

    return valueCollection;
  }

  /** {@inheritDoc} */
  public EntrySet entrySet() {
    if (null == entrySet) {
      entrySet = new EntrySet();
    }

    return entrySet;
  }

  /** {@inheritDoc} */
  public String toString() {
    if (isEmpty()) {
      return "{}";
    }

    final EntryIterator entryIterator = new EntryIterator();
    entryIterator.reset();

    final StringBuilder sb = new StringBuilder().append('{');
    while (true) {
      entryIterator.next();
      sb.append(entryIterator.getIntKey())
          .append('=')
          .append(unmapNullValue(entryIterator.getValue()));
      if (!entryIterator.hasNext()) {
        return sb.append('}').toString();
      }
      sb.append(',').append(' ');
    }
  }

  /** {@inheritDoc} */
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof Map)) {
      return false;
    }

    final Map<?, ?> that = (Map<?, ?>) o;

    if (size != that.size()) {
      return false;
    }

    for (int i = 0, length = values.length(); i < length; i++) {
      final Object thisValue = values.get(i);
      if (null != thisValue) {
        final Object thatValue = that.get(keys.get(i));
        if (!thisValue.equals(mapNullValue(thatValue))) {
          return false;
        }
      }
    }

    return true;
  }

  /** {@inheritDoc} */
  public int hashCode() {
    int result = 0;

    for (int i = 0, length = values.length(); i < length; i++) {
      final Object value = values.get(i);
      if (null != value) {
        result += (Integer.hashCode(keys.get(i)) ^ value.hashCode());
      }
    }

    return result;
  }

  protected Object mapNullValue(final Object value) {
    return value;
  }

  @SuppressWarnings("unchecked")
  protected V unmapNullValue(final Object value) {
    return (V) value;
  }

  /**
   * Primitive specialised version of {@link #replace(Object, Object)}
   *
   * @param key key with which the specified value is associated
   * @param value value to be associated with the specified key
   * @return the previous value associated with the specified key, or {@code null} if there was no
   *     mapping for the key.
   */
  public V replace(final int key, final V value) {
    V curValue = get(key);
    if (curValue != null) {
      curValue = put(key, value);
    }

    return curValue;
  }

  /**
   * Primitive specialised version of {@link #replace(Object, Object, Object)}
   *
   * @param key key with which the specified value is associated
   * @param oldValue value expected to be associated with the specified key
   * @param newValue value to be associated with the specified key
   * @return {@code true} if the value was replaced
   */
  public boolean replace(final int key, final V oldValue, final V newValue) {
    final Object curValue = get(key);
    if (curValue == null || !Objects.equals(unmapNullValue(curValue), oldValue)) {
      return false;
    }

    put(key, newValue);

    return true;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////
  // Internal Sets and Collections
  ///////////////////////////////////////////////////////////////////////////////////////////////

  private void increaseCapacity() {
    final int newCapacity = values.length() << 1;
    if (newCapacity < 0) {
      throw new IllegalStateException("max capacity reached at size=" + size);
    }

    rehash(newCapacity);
  }

  private void rehash(final int newCapacity) {
    final int mask = newCapacity - 1;
    /*  */ resizeThreshold = (int) (newCapacity * loadFactor);

    final AtomicIntegerArray tempKeys = new AtomicIntegerArray(newCapacity);
    final AtomicReferenceArray tempValues = new AtomicReferenceArray(newCapacity);

    for (int i = 0, size = values.length(); i < size; i++) {
      final Object value = values.get(i);
      if (null != value) {
        final int key = keys.get(i);
        int index = hash(key, mask);
        while (null != tempValues.get(index)) {
          index = ++index & mask;
        }

        tempKeys.set(index, key);
        tempValues.set(index, value);
      }
    }

    keys = tempKeys;
    values = tempValues;
  }

  @SuppressWarnings("FinalParameters")
  private void compactChain(int deleteIndex) {
    final int mask = values.length() - 1;
    int index = deleteIndex;
    while (true) {
      index = ++index & mask;
      if (null == values.get(index)) {
        break;
      }

      final int hash = hash(keys.get(index), mask);

      if ((index < hash && (hash <= deleteIndex || deleteIndex <= index))
          || (hash <= deleteIndex && deleteIndex <= index)) {
        
        keys.set(deleteIndex, keys.get(index));
        values.set(deleteIndex, values.get(index));
        values.set(index, null);
        deleteIndex = index;
      }
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////
  // Iterators
  ///////////////////////////////////////////////////////////////////////////////////////////////

  public final class KeySet extends AbstractSet<Integer> implements Serializable {
    private final KeyIterator keyIterator = shouldAvoidAllocation ? new KeyIterator() : null;

    /** {@inheritDoc} */
    public KeyIterator iterator() {
      KeyIterator keyIterator = this.keyIterator;
      if (null == keyIterator) {
        keyIterator = new KeyIterator();
      }

      keyIterator.reset();
      return keyIterator;
    }

    public int size() {
      return AtomicInt2ObjectHashMap.this.size();
    }

    public boolean contains(final Object o) {
      return AtomicInt2ObjectHashMap.this.containsKey(o);
    }

    public boolean contains(final int key) {
      return AtomicInt2ObjectHashMap.this.containsKey(key);
    }

    public boolean remove(final Object o) {
      return null != AtomicInt2ObjectHashMap.this.remove(o);
    }

    public boolean remove(final int key) {
      return null != AtomicInt2ObjectHashMap.this.remove(key);
    }

    public void clear() {
      AtomicInt2ObjectHashMap.this.clear();
    }
  }

  public final class ValueCollection extends AbstractCollection<V> implements Serializable {
    private final ValueIterator valueIterator = shouldAvoidAllocation ? new ValueIterator() : null;

    /** {@inheritDoc} */
    public ValueIterator iterator() {
      ValueIterator valueIterator = this.valueIterator;
      if (null == valueIterator) {
        valueIterator = new ValueIterator();
      }

      valueIterator.reset();
      return valueIterator;
    }

    public int size() {
      return AtomicInt2ObjectHashMap.this.size();
    }

    public boolean contains(final Object o) {
      return AtomicInt2ObjectHashMap.this.containsValue(o);
    }

    public void clear() {
      AtomicInt2ObjectHashMap.this.clear();
    }
  }

  public final class EntrySet extends AbstractSet<Entry<Integer, V>> implements Serializable {
    private final EntryIterator entryIterator = shouldAvoidAllocation ? new EntryIterator() : null;

    /** {@inheritDoc} */
    public EntryIterator iterator() {
      EntryIterator entryIterator = this.entryIterator;
      if (null == entryIterator) {
        entryIterator = new EntryIterator();
      }

      entryIterator.reset();
      return entryIterator;
    }

    public int size() {
      return AtomicInt2ObjectHashMap.this.size();
    }

    public void clear() {
      AtomicInt2ObjectHashMap.this.clear();
    }

    /** {@inheritDoc} */
    public boolean contains(final Object o) {
      final Entry entry = (Entry) o;
      final int key = (Integer) entry.getKey();
      final V value = getMapped(key);
      return value != null && value.equals(mapNullValue(entry.getValue()));
    }
  }

  abstract class AbstractIterator<T> implements Iterator<T>, Serializable {
    boolean isPositionValid = false;
    private int posCounter;
    private int stopCounter;
    private int remaining;

    protected final int position() {
      return posCounter & (values.length() - 1);
    }

    public int remaining() {
      return remaining;
    }

    public boolean hasNext() {
      return remaining > 0;
    }

    protected final void findNext() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      final AtomicReferenceArray values = AtomicInt2ObjectHashMap.this.values;
      final int mask = values.length() - 1;

      for (int i = posCounter - 1; i >= stopCounter; i--) {
        final int index = i & mask;
        if (null != values.get(index)) {
          posCounter = i;
          isPositionValid = true;
          --remaining;
          return;
        }
      }

      isPositionValid = false;
      throw new IllegalStateException();
    }

    public abstract T next();

    public void remove() {
      if (isPositionValid) {
        final int position = position();
        values.set(position, null);
        --size;

        compactChain(position);

        isPositionValid = false;
      } else {
        throw new IllegalStateException();
      }
    }

    final void reset() {
      remaining = AtomicInt2ObjectHashMap.this.size;
      final AtomicReferenceArray values = AtomicInt2ObjectHashMap.this.values;
      final int capacity = values.length();

      int i = capacity;
      if (null != values.get(capacity - 1)) {
        for (i = 0; i < capacity; i++) {
          if (null == values.get(i)) {
            break;
          }
        }
      }

      stopCounter = i;
      posCounter = i + capacity;
      isPositionValid = false;
    }
  }

  public class ValueIterator extends AbstractIterator<V> {
    @SuppressWarnings("unchecked")
    public V next() {
      findNext();

      return unmapNullValue(values.get(position()));
    }
  }

  public class KeyIterator extends AbstractIterator<Integer> {
    public Integer next() {
      return nextInt();
    }

    public int nextInt() {
      findNext();

      return keys.get(position());
    }
  }

  public class EntryIterator extends AbstractIterator<Entry<Integer, V>>
      implements Entry<Integer, V> {
    public Entry<Integer, V> next() {
      findNext();
      if (shouldAvoidAllocation) {
        return this;
      }

      return allocateDuplicateEntry();
    }

    private Entry<Integer, V> allocateDuplicateEntry() {
      final int k = getIntKey();
      final V v = getValue();

      return new Entry<Integer, V>() {
        public Integer getKey() {
          return k;
        }

        public V getValue() {
          return v;
        }

        public V setValue(final V value) {
          return AtomicInt2ObjectHashMap.this.put(k, value);
        }

        public int hashCode() {
          return Integer.hashCode(getIntKey()) ^ (v != null ? v.hashCode() : 0);
        }

        public boolean equals(final Object o) {
          if (!(o instanceof Entry)) {
            return false;
          }

          final Entry e = (Entry) o;

          return (e.getKey() != null && e.getKey().equals(k))
              && ((e.getValue() == null && v == null) || e.getValue().equals(v));
        }

        public String toString() {
          return k + "=" + v;
        }
      };
    }

    public Integer getKey() {
      return getIntKey();
    }

    public int getIntKey() {
      return keys.get(position());
    }

    public V getValue() {
      return unmapNullValue(values.get(position()));
    }

    @SuppressWarnings("unchecked")
    public V setValue(final V value) {
      final V val = (V) mapNullValue(value);
      requireNonNull(val, "value cannot be null");

      if (!this.isPositionValid) {
        throw new IllegalStateException();
      }

      final int pos = position();
      final Object oldValue = values.get(pos);
      values.set(pos, val);

      return (V) oldValue;
    }
  }
}
