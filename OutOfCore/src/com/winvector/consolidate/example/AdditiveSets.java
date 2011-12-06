package com.winvector.consolidate.example;

import java.util.ArrayList;
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
import com.winvector.consolidate.impl.ISetPairAdapter;
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
		final DataAdapter<ArrayList<SortedSet<Integer>>> pairadapter = new ISetPairAdapter(adapter);
		@SuppressWarnings("unchecked")
		final RelnCollector<SortedSet<Integer>,ArrayList<SortedSet<Integer>>>[] collectors = new RelnCollector[] {
			new FileRelnCollector<SortedSet<Integer>,ArrayList<SortedSet<Integer>>>(adapter,pairadapter,"/usr/bin/sort"),
			new InMemoryRelnCollector<SortedSet<Integer>,ArrayList<SortedSet<Integer>>>(adapter,pairadapter),
		};
		for(final RelnCollector<SortedSet<Integer>,ArrayList<SortedSet<Integer>>> collector: collectors) {
			final Date start = new Date();
			final String collectorName = collector.getClass().getCanonicalName();
			System.out.println("Start\t" + collectorName + "\t" + start);
			final Iterable<SortedSet<Integer>> sets = new KSets(n,k);
			long nReln = 0;
			for(final SortedSet<Integer> s1: sets) {
				for(final SortedSet<Integer> s2: sets) {
					final SortedSet<Integer> sum = sum(s1,s2,mod);
					final ArrayList<SortedSet<Integer>> pair = new ArrayList<SortedSet<Integer>>(2);
					pair.add(s1);
					pair.add(s2);
					collector.insertReln(sum,pair);
					//System.out.println("\t" + s1 + "\t" + s2 + "\t" + sum);
					++nReln;
				}
			}
			System.out.println("\tInserted " + nReln + " relations.");
			// get distribution of number of summands
			long nSums = 0;
			long nSummands = 0;
			final Iterable<Entry<SortedSet<Integer>, Iterable<ArrayList<SortedSet<Integer>>>>> solns = collector.entries();
			final SortedMap<Integer,Integer> counts = new TreeMap<Integer,Integer>();
			int minSize = Integer.MAX_VALUE;
			SortedSet<Integer> target = null;
			SortedSet<ArrayList<SortedSet<Integer>>> pairs = null;
			for(final Entry<SortedSet<Integer>, Iterable<ArrayList<SortedSet<Integer>>>> me: solns) {
				// copy values into a set
				final Iterable<ArrayList<SortedSet<Integer>>> summandsIt = me.getValue();
				final SortedSet<ArrayList<SortedSet<Integer>>> summands = new TreeSet<ArrayList<SortedSet<Integer>>>(pairadapter);
				for(final ArrayList<SortedSet<Integer>> v: summandsIt) {
					summands.add(v);
				}
				if((target==null)||(summands.size()<minSize)) {
					minSize = summands.size();
					target = me.getKey();
					pairs = summands;
				}
				nSums += 1;
				nSummands += summands.size();
				final Integer tally = counts.get(summands.size());
				counts.put(summands.size(),null==tally?1:tally+1);
			}
			{
				for(final ArrayList<SortedSet<Integer>> v: pairs) {
					System.out.println(" " + v.get(0) + " + " + v.get(1) + " = " + target);
				}
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
