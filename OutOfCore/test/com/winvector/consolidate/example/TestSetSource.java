package com.winvector.consolidate.example;

import static org.junit.Assert.*;

import java.util.SortedSet;

import org.junit.Test;

public class TestSetSource {
	@Test
	public void testKSets() {
		final int n = 5;
		final int k = 2;
		final int nChooseK = 10;
		final Iterable<SortedSet<Integer>> source = new KSets(n,k);
		int got = 0;
		for(final SortedSet<Integer> row: source) {
			//System.out.println(row);
			assertEquals(k,row.size());
			for(final int v: row) {
				assertTrue(v>=0);
				assertTrue(v<n);
			}
			++got;
		}
		assertEquals(nChooseK,got);
	}
}
