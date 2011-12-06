package com.winvector.consolidate.example;

import java.util.Date;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.winvector.consolidate.def.DataAdapter;
import com.winvector.consolidate.def.RelnCollector;
import com.winvector.consolidate.impl.FileRelnCollector;
import com.winvector.consolidate.impl.ISetAdapter;
import com.winvector.consolidate.impl.InMemoryRelnCollector;

public final class AdditiveSets {
	
	public static SortedSet<Integer> sum(final SortedSet<Integer> a, final SortedSet<Integer> b, final int mod) {
		final SortedSet<Integer> r = new TreeSet<Integer>();
		for(final int av: a) {
			for(final int bv: b) {
				final int sum = (av+bv)%mod;
				r.add(sum);
			}
		}
		return r;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		final int n = 19;
		final int mod = n;
		final int k = (int)Math.floor(Math.sqrt(n));
		System.out.println("Examining sums of " + k + " integers chosen from 0 through " + (n-1) + " modulo " + mod + ".");
		final DataAdapter<SortedSet<Integer>> adapter = new ISetAdapter();
		@SuppressWarnings("unchecked")
		final RelnCollector<SortedSet<Integer>,SortedSet<Integer>>[] collectors = new RelnCollector[] {
			new FileRelnCollector<SortedSet<Integer>,SortedSet<Integer>>(adapter,adapter,"/usr/bin/sort"),
			new InMemoryRelnCollector<SortedSet<Integer>,SortedSet<Integer>>(adapter,adapter),
		};
		for(final RelnCollector<SortedSet<Integer>,SortedSet<Integer>> collector: collectors) {
			final Date start = new Date();
			final String collectorName = collector.getClass().getCanonicalName();
			System.out.println("Start\t" + collectorName + "\t" + start);
			final Iterable<SortedSet<Integer>> sets = new KSets(n,k);
			long nReln = 0;
			for(final SortedSet<Integer> s1: sets) {
				for(final SortedSet<Integer> s2: sets) {
					final SortedSet<Integer> sum = sum(s1,s2,mod);
					collector.insertReln(sum,s1);
					//System.out.println("\t" + s1 + "\t" + s2 + "\t" + sum);
					++nReln;
				}
			}
			System.out.println("\tInserted " + nReln + " relations.");
			// get distribution of number of summands
			long nSums = 0;
			long nSummands = 0;
			final Iterable<Entry<SortedSet<Integer>,Iterable<SortedSet<Integer>>>> solns = collector.entries();
			final SortedMap<Integer,Integer> counts = new TreeMap<Integer,Integer>();
			for(final Entry<SortedSet<Integer>,Iterable<SortedSet<Integer>>> me: solns) {
				// copy values into a set
				final Iterable<SortedSet<Integer>> summandsIt = me.getValue();
				final SortedSet<SortedSet<Integer>> summands = new TreeSet<SortedSet<Integer>>(adapter);
				for(final SortedSet<Integer> v: summandsIt) {
					summands.add(v);
				}
				nSums += 1;
				nSummands += summands.size();
				final Integer tally = counts.get(summands.size());
				counts.put(summands.size(),null==tally?1:tally+1);
			}
			collector.close();
			System.out.println("\tExamined " + nSums + " sums and " + nSummands + " summands.");
			for(final Entry<Integer, Integer> me: counts.entrySet()) {
				System.out.println("\tfound " + me.getValue() + " sums with " + me.getKey() + " distinct summands");
			}
			final Date end = new Date();
			final long elapsedMS = end.getTime() - start.getTime();
			System.out.println("Done:\t" + collectorName + "\telapsed time: " + elapsedMS + "MS\t" + end);
		}
	}
}
