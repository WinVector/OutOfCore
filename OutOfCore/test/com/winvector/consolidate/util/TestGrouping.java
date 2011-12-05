package com.winvector.consolidate.util;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.Test;

import com.winvector.consolidate.impl.GroupingIterable;
import com.winvector.consolidate.impl.ME;

public class TestGrouping {
	
	public static final Set<String> set(final String[] v) {
		final Set<String> s = new TreeSet<String>();
		for(final String vi: v) {
			s.add(vi);
		}
		return s;
	}
	
	public static <A,B,C extends Iterable<B>> boolean equals(final Map<A,Set<B>> expect, final Iterable<Map.Entry<A,C>> got) {
		final int ne = expect.size();
		int ng = 0;
		for(final Map.Entry<A,C> gi: got) {
			final A key = gi.getKey();
			final C setg = gi.getValue();
			final Set<B> ei = expect.get(key);
			if(ei==null) {
				return false; // A missing a set
			}
			final int sze = ei.size();
			int szg = 0;
			for(final B vg: setg) {
				if(!ei.contains(vg)) {
					return false; // mismatch
				}
				++szg;
				if(szg>sze) {
					return false; // sets different size
				}
			}
			if(sze!=szg) {
				return false; // sets different size
			}
			++ng;
			if(ng>ne) {
				return false; // more got rows than expect
			}
		}
		if(ng!=ne) {
			return false; // number of rows mismatches
		}
		return true;
	}


	@Test
	public void testGrouping() {
		final ArrayList<ME<String,String>> dat = new ArrayList<ME<String,String>>();
		dat.add(new ME<String,String>("a", "b")); // a's in blocks as we are not calling sort
		dat.add(new ME<String,String>("a","a"));
		dat.add(new ME<String,String>("b", "a"));
		final Map<String,Set<String>> expect = new TreeMap<String,Set<String>>();
		expect.put("a",set(new String[] {"a","b"}));
		expect.put("b",set(new String[] {"a"}));
		final Comparator<String> scomp = new Comparator<String>() {
			@Override
			public int compare(String arg0, String arg1) {
				return arg0.compareTo(arg1);
			}
		};
		final Iterable<Map.Entry<String,Iterable<String>>> source = new GroupingIterable<String,String>(scomp,scomp,dat);
		final boolean eq = TestGrouping.equals(expect,source);
		assertTrue(eq);
	}



}
