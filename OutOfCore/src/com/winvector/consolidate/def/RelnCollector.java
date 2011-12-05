package com.winvector.consolidate.def;

import java.io.IOException;
import java.util.Map;

/**
 * Collects relation entries of the form (A,B)
 * then can return grouped results A,Set<B> (clears collecting structure).
 * Call close() to ensure all reasearches are released
 * @author jmount
 *
 * @param <A>
 * @param <B>
 */
public interface RelnCollector<A,B> {
	void insertReln(A a, B b) throws IOException;
	Iterable<Map.Entry<A,Iterable<B>>> entries() throws IOException, InterruptedException;
	void close() throws IOException;
}
