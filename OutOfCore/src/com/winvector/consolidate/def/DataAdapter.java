package com.winvector.consolidate.def;

import java.util.Comparator;

public interface DataAdapter<K> extends Comparator<K> {
	/**
	 * 
	 * @param k
	 * @return encoding of k, free of tabs and line-breaks
	 */
	public String toString(K k);
	
	/**
	 * Inverse of toString()
	 * @param s
	 * @return
	 */
	public K parse(String s);
}
