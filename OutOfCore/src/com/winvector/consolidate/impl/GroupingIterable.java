package com.winvector.consolidate.impl;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;


/**
 * 
 * Convert a Iteratable over Map.Entry<A,B> to an Iterable over Map.Entry<A,Iterable<B>>
 * the source Iterable must present all A in blocks (for example sorted with respect
 * to As would be good enough).
 * 
 * @author jmount
 *
 * @param <A>
 * @param <B>
 */
public final class GroupingIterable<A,B> implements Iterable<Map.Entry<A,Iterable<B>>> {
	private final Comparator<A> compA;
	private final Comparator<B> compB;
	private final Iterable<? extends Map.Entry<A,B>> source;
	
	/**
	 * 
	 * @param compA
	 * @param compB
	 * @param source presents keys with all As in blocks (for example sorted with respect to As is good enough)
	 */
	public GroupingIterable(final Comparator<A> compA, final Comparator<B> compB,
			final Iterable<? extends Map.Entry<A,B>> source) {
		this.compA = compA;
		this.compB = compB;
		this.source = source;
	}
	
	private final class GroupingIterator implements Iterator<Entry<A,Iterable<B>>> {
		private final Iterator<? extends Map.Entry<A,B>> underlying;
		private Map.Entry<A,B> nextPair = null;
		
		public GroupingIterator(final Iterator<? extends Map.Entry<A,B>> underlying) {
			this.underlying = underlying;
			if(underlying.hasNext()) {
				nextPair = underlying.next();
			}
		}
		
		private Entry<A,Iterable<B>> advance() throws IOException {
			if(nextPair!=null) {
				// collect everything with this key
				final A key = nextPair.getKey();
				final Set<B> values = new TreeSet<B>(compB);
				values.add(nextPair.getValue());
				while(true) {
					if(underlying.hasNext()) {
						nextPair = underlying.next();
						if(compA.compare(key,nextPair.getKey())!=0) {
							break;
						}
						values.add(nextPair.getValue());
					} else {
						nextPair = null;
						break;
					}
				}
				return new ME<A,Iterable<B>>(key,values);
			}
			return null;
		}
		
		@Override
		public boolean hasNext() {
			return nextPair!=null;
		}

		@Override
		public Entry<A,Iterable<B>> next() {
			if(!hasNext()) {
				throw new NoSuchElementException();
			}
			try {
				return advance();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	
	@Override
	public Iterator<Entry<A,Iterable<B>>> iterator() {
		return new GroupingIterator(source.iterator());
	}
}
