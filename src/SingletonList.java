/*
 * Copyright (c) 2010  StumbleUpon, Inc.  All rights reserved.
 * This file is part of Async HBase.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *   - Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   - Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *   - Neither the name of the StumbleUpon nor the names of its contributors
 *     may be used to endorse or promote products derived from this software
 *     without specific prior written permission.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.hbase.async;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

/**
 * An immutable {@link List} that holds only a single non-{@code null} value.
 * @param <E> The type of the singleton element.
 */
final class SingletonList<E> implements List<E> {

  private final E element;

  /**
   * Constructor.
   * @param element The only element that will ever be in this list.
   * Must not be {@code null}.
   */
  public SingletonList(final E element) {
    if (element == null) {
      throw new NullPointerException("element");
    }
    this.element = element;
  }

  public String toString() {
    return "[" + element + ']';
  }

  @Override
  public int size() {
    return 1;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public boolean contains(final Object o) {
    return element.equals(o);
  }

  @Override
  public Iterator<E> iterator() {
    return new Iter<E>(element);
  }

  @Override
  public Object[] toArray() {
    final Object[] array = new Object[1];
    array[0] = element;
    return array;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T[] toArray(final T[] a) {
    if (a.length < 1) {
      return (T[]) toArray();
    }
    a[0] = (T) element;
    if (a.length > 1) {
      a[1] = null;
    }
    return a;
  }

  @Override
  public boolean add(final E e) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean remove(final Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsAll(final Collection<?> c) {
    if (c.size() != 1) {
      return false;
    }
    return c.contains(element);
  }

  @Override
  public boolean addAll(final Collection<? extends E> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addAll(final int index, final Collection<? extends E> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeAll(final Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean retainAll(final Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || !(other instanceof SingletonList)) {
      return false;
    }
    return element.equals(((SingletonList) other).element);
  }

  @Override
  public int hashCode() {
    return 31 + element.hashCode();
  }

  @Override
  public E get(final int index) {
    if (index != 0) {
      throw new IndexOutOfBoundsException("only 1 element but index=" + index);
    }
    return element;
  }

  @Override
  public E set(final int index, final E element) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void add(final int index, final E element) {
    throw new UnsupportedOperationException();
  }

  @Override
  public E remove(final int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int indexOf(final Object o) {
    return o.equals(element) ? 0 : -1;
  }

  @Override
  public int lastIndexOf(Object o) {
    return o.equals(element) ? 0 : -1;
  }

  @Override
  public ListIterator<E> listIterator() {
    return new Iter<E>(element);
  }

  @Override
  public ListIterator<E> listIterator(final int index) {
    if (index != 0) {
      throw new IndexOutOfBoundsException("only 1 element but index=" + index);
    }
    return new Iter<E>(element);
  }

  @Override
  public List<E> subList(final int fromIndex, final int toIndex) {
    if (fromIndex == 0 && toIndex == 1) {
      return this;
    }
    throw new IndexOutOfBoundsException("only 1 element but requested ["
                                        + fromIndex + "; " + toIndex + ']');
  }

  private static final class Iter<E> implements ListIterator<E> {

    private final E element;
    private boolean returned = false;

    public Iter(final E element) {
      this.element = element;
    }

    @Override
    public boolean hasNext() {
      return !returned;
    }

    @Override
    public E next() {
      if (returned) {
        throw new NoSuchElementException();
      }
      returned = true;
      return element;
    }

    @Override
    public boolean hasPrevious() {
      return returned;
    }

    @Override
    public E previous() {
      if (!returned) {
        throw new NoSuchElementException();
      }
      returned = false;
      return element;
    }

    @Override
    public int nextIndex() {
      return returned ? 0 : 1;
    }

    @Override
    public int previousIndex() {
      return returned ? -1 : 0;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void set(final E e) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void add(final E e) {
      throw new UnsupportedOperationException();
    }

  }

}
