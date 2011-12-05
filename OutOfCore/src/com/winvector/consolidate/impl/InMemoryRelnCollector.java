package com.winvector.consolidate.impl;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.winvector.consolidate.def.DataAdapter;
import com.winvector.consolidate.def.RelnCollector;

public final class InMemoryRelnCollector<A,B> implements RelnCollector<A,B> {
	private final DataAdapter<A> adapterA;
	private final DataAdapter<B> adapterB;
	private Map<A,Iterable<B>> atoBs;
	
	public InMemoryRelnCollector(final DataAdapter<A> adapterA, final DataAdapter<B> adapterB) {
		this.adapterA = adapterA;
		this.adapterB = adapterB;
		atoBs = new TreeMap<A,Iterable<B>>(this.adapterA);
	}
	
	@Override
	public void insertReln(final A a, final B b) {
		Set<B> set = (Set<B>) atoBs.get(a);
		if(null==set) {
			set = new TreeSet<B>(adapterB);
			atoBs.put(a,set);
		}
		if(!set.contains(b)) {
			set.add(b);
		}
	}

	@Override
	public Iterable<Map.Entry<A,Iterable<B>>> entries() {
		return atoBs.entrySet();
	}

	@Override
	public void close() {
		atoBs = null;
	}
}
