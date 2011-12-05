package com.winvector.consolidate.example;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Generate all k-subsets of {0,,,n-1}
 * @author jmount
 *
 */
public final class KSets implements Iterable<SortedSet<Integer>> {
	public final int n;
	public final int k;
	
	public KSets(final int n, final int k) {
		this.n = n;
		this.k = k;
	}
	
	private final class SetIt implements Iterator<SortedSet<Integer>> {
		private int[] posns = new int[k];
		
		public SetIt() {
			for(int i=0;i<k;++i) {
				posns[i] = i;
			}
		}
		
		private void advance() {
			// find right most advanceable position
			if(k>0) {
				int advanceable = k-1;
				if(posns[advanceable]>=n-1) {
					--advanceable;
					while((advanceable>=0)&&(posns[advanceable]+1>=posns[advanceable+1])) {
						--advanceable;
					}
				}
				if(advanceable>=0) {
					posns[advanceable] += 1;
					for(int p=advanceable+1;p<k;++p) {
						posns[p] = posns[p-1]+1;
					}
				} else {
					posns = null;
				}
			} else {
				posns = null;
			}
		}

		@Override
		public boolean hasNext() {
			return posns!=null;
		}

		@Override
		public SortedSet<Integer> next() {
			if(!hasNext()) {
				throw new NoSuchElementException();
			}
			final SortedSet<Integer> ret = new TreeSet<Integer>();
			for(final int ii: posns) {
				ret.add(ii);
			}
			advance();
			return ret;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	@Override
	public Iterator<SortedSet<Integer>> iterator() {
		return new SetIt();
	}
}
