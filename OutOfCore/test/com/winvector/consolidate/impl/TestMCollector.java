package com.winvector.consolidate.impl;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.Test;

import com.winvector.consolidate.def.DataAdapter;
import com.winvector.consolidate.def.RelnCollector;
import com.winvector.consolidate.util.DBUtil;
import com.winvector.consolidate.util.DBUtil.DBHandle;
import com.winvector.consolidate.util.TestGrouping;

public class TestMCollector {
	
	public static final Set<Integer> set(final int[] v) {
		final Set<Integer> s = new TreeSet<Integer>();
		for(final int vi: v) {
			s.add(vi);
		}
		return s;
	}
	
	@Test
	public void testMem() throws IOException, InterruptedException {
		final RelnCollector<Integer,Integer> c = new InMemoryRelnCollector<Integer,Integer>(new IntegerAdapter(), new IntegerAdapter());
		c.insertReln(2,2);
		c.insertReln(1,3);
		c.insertReln(2,4);
		c.insertReln(1,5);
		c.insertReln(2,2);
		final Map<Integer,Set<Integer>> expect = new TreeMap<Integer,Set<Integer>>();
		expect.put(1,set(new int[] {3,5}));
		expect.put(2,set(new int[] {2,4}));
		final boolean eq = TestGrouping.equals(expect,c.entries());
		c.close();
		assertTrue(eq);
	}

	@Test
	public void testF() throws IOException, InterruptedException {
		final RelnCollector<Integer,Integer> c = new FileRelnCollector<Integer,Integer>(new IntegerAdapter(), new IntegerAdapter(),"/usr/bin/sort");
		c.insertReln(2,2);
		c.insertReln(1,3);
		c.insertReln(2,4);
		c.insertReln(1,5);
		c.insertReln(2,2);
		final Map<Integer,Set<Integer>> expect = new TreeMap<Integer,Set<Integer>>();
		expect.put(1,set(new int[] {3,5}));
		expect.put(2,set(new int[] {2,4}));
		final boolean eq = TestGrouping.equals(expect,c.entries());
		c.close();
		assertTrue(eq);
	}
	
	@Test
	public void testD() throws IOException, InterruptedException, SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		final String comment = "test";
		final String dbUserName = "";
		final String dbPassword = "";
		final String driver = "org.h2.Driver";
		final File tmpFile = File.createTempFile("TestH2DBC",".dir");
		tmpFile.delete();
		tmpFile.mkdirs();
		final String dbURL = "jdbc:h2:/" + (new File(tmpFile,"H2DB")).getAbsolutePath() + ";LOG=0;CACHE_SIZE=65536;LOCK_MODE=0;UNDO_LOG=0";
		final boolean readOnly = false;
		final DBHandle handle = DBUtil.buildConnection(comment,
				dbUserName,
				dbPassword,
				dbURL,
				driver,
				readOnly);
		final RelnCollector<Integer,Integer> c = new DBRelnCollector<Integer,Integer>(new IntegerAdapter(), new IntegerAdapter(),handle,100);
		c.insertReln(2,2);
		c.insertReln(1,3);
		c.insertReln(2,4);
		c.insertReln(1,5);
		c.insertReln(2,2);
		final Map<Integer,Set<Integer>> expect = new TreeMap<Integer,Set<Integer>>();
		expect.put(1,set(new int[] {3,5}));
		expect.put(2,set(new int[] {2,4}));
		final boolean eq = TestGrouping.equals(expect,c.entries());
		c.close();
		handle.conn.close();
		// clean up
		for(final File ci: tmpFile.listFiles()) {
			ci.delete();
		}
		tmpFile.delete();
		assertTrue(eq);
	}

	@Test
	public void testMR() throws IOException, InterruptedException {
		final RelnCollector<Integer,Integer> c = new MapReduceRelnCollector<Integer,Integer>(new IntegerAdapter(), new IntegerAdapter());
		c.insertReln(2,2);
		c.insertReln(1,3);
		c.insertReln(2,4);
		c.insertReln(1,5);
		c.insertReln(2,2);
		final Map<Integer,Set<Integer>> expect = new TreeMap<Integer,Set<Integer>>();
		expect.put(1,set(new int[] {3,5}));
		expect.put(2,set(new int[] {2,4}));
		final boolean eq = TestGrouping.equals(expect,c.entries());
		c.close();
		assertTrue(eq);
	}

	
	private static <A,B,C extends Iterable<B>> Map<A,Set<B>> copyOut(final Iterable<Map.Entry<A,C>> got, final DataAdapter<A> adapterA, final DataAdapter<B> adapterB) {
		final Map<A,Set<B>> r = new TreeMap<A,Set<B>>(adapterA);
		for(Map.Entry<A,C> me: got) {
			final Set<B> bdat = new TreeSet<B>(adapterB);
			for(final B v: me.getValue()) {
				bdat.add(v);
			}
			r.put(me.getKey(),bdat);
		}
		return r;
	}
	
	@Test
	public void testMF() throws IOException, InterruptedException {
		final RelnCollector<Integer,Integer> cm = new InMemoryRelnCollector<Integer,Integer>(new IntegerAdapter(), new IntegerAdapter());
		final RelnCollector<Integer,Integer> cf = new FileRelnCollector<Integer,Integer>(new IntegerAdapter(), new IntegerAdapter(),"/usr/bin/sort");
		final Random rand = new Random(53223);
		for(int i=0;i<1000;++i) {
			final int a = rand.nextInt(100);
			final int b = rand.nextInt(100);
			cm.insertReln(a, b);
			cf.insertReln(a, b);
		}
		final Map<Integer,Set<Integer>> av = copyOut(cm.entries(),new IntegerAdapter(),new IntegerAdapter());
		final boolean eq = TestGrouping.equals(av,cf.entries());
		cm.close();
		cf.close();
		assertTrue(eq);
	}
}
