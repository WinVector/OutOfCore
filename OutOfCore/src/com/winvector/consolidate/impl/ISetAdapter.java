package com.winvector.consolidate.impl;

import java.io.Serializable;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import com.winvector.consolidate.def.DataAdapter;


public final class ISetAdapter implements DataAdapter<SortedSet<Integer>>, Serializable {
	private static final long serialVersionUID = 1L;

	@Override
	public int compare(final SortedSet<Integer> arg0, final SortedSet<Integer> arg1) {
		final int sz0 = arg0.size();
		final int sz1 = arg1.size();
		if(sz0!=sz1) {
			if(sz0<sz1) {
				return -1;
			} else {
				return 1;
			}
		}
		final Iterator<Integer> it0 = arg0.iterator();
		final Iterator<Integer> it1 = arg1.iterator();
		while(it0.hasNext()) {
			final int v0 = it0.next();
			final int v1 = it1.next();
			if(v0!=v1) {
				if(v0<v1) {
					return -1;
				} else {
					return 1;
				}
			}
		}
		return 0;
	}
	

	@Override
	public String toString(SortedSet<Integer> k) {
		final StringBuilder b = new StringBuilder();
		b.append("{");
		boolean first = true;
		for(final int vi: k) {
			if(first) {
				first = false;
			} else {
				b.append(",");
			}
			b.append(vi);
		}
		b.append("}");
		return b.toString();
	}

	@Override
	public SortedSet<Integer> parse(final String origs) {
		String s = origs.replaceAll("\\{","");
		s = s.replaceAll("\\}","");
		final String flds[] = s.split(",+");
		final SortedSet<Integer> r = new TreeSet<Integer>();
		for(final String fi: flds) {
			if(fi.length()>0) {
				final Integer vi = Integer.parseInt(fi);
				r.add(vi);
			}
		}
		return r;
	}
};
