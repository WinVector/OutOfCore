package com.winvector.consolidate.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.SortedSet;

import com.winvector.consolidate.def.DataAdapter;

public final class ISetPairAdapter implements DataAdapter<ArrayList<SortedSet<Integer>>>, Serializable {
	private static final long serialVersionUID = 1L;
	
	private final DataAdapter<SortedSet<Integer>> underlying;
	
	/**
	 * 
	 * @param underlying an datapter not using semicolons or square brackets in its encoding
	 */
	public ISetPairAdapter(final DataAdapter<SortedSet<Integer>> underlying) {
		this.underlying = underlying;
	}

	@Override
	public int compare(final ArrayList<SortedSet<Integer>> o1,
			final ArrayList<SortedSet<Integer>> o2) {
		final int n = o1.size();
		if(n!=o2.size()) {
			if(n>=o2.size()) {
				return 1;
			} else {
				return -1;
			}
		}
		for(int i=0;i<n;++i) {
			final int cmp = underlying.compare(o1.get(i),o2.get(i));
			if(cmp!=0) {
				return cmp;
			}
		}
		return 0;
	}

	@Override
	public String toString(ArrayList<SortedSet<Integer>> k) {
		final StringBuilder b = new StringBuilder();
		b.append('[');
		final int n = k.size();
		for(int i=0;i<n;++i) {
			if(i>0) {
				b.append(";");
			}
			b.append(underlying.toString(k.get(i)));
		}
		b.append(']');
		return b.toString();
	}

	@Override
	public ArrayList<SortedSet<Integer>> parse(final String origs) {
		String s = origs.replaceAll("\\[","");
		s = s.replaceAll("\\]","");
		final String flds[] = s.split(";+");
		final int n = flds.length;
		final ArrayList<SortedSet<Integer>> r = new ArrayList<SortedSet<Integer>>(n);
		for(int i=0;i<n;++i) {
			r.add(underlying.parse(flds[i]));
		}
		return r;
	}
}
