package com.winvector.consolidate.impl;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Random;

import com.winvector.consolidate.def.DataAdapter;
import com.winvector.consolidate.def.RelnCollector;
import com.winvector.consolidate.util.BurstMap;
import com.winvector.consolidate.util.DBIterable;
import com.winvector.consolidate.util.DBUtil.DBHandle;

public class DBRelnCollector<A,B> implements RelnCollector<A,B> {
	private final DataAdapter<A> adapterA;
	private final DataAdapter<B> adapterB;
	private final String tableName;
	private Statement stmt;
	private PreparedStatement insertStmt;
	private PreparedStatement deleteStmt;

	
	public DBRelnCollector(final DataAdapter<A> adapterA, final DataAdapter<B> adapterB,
			final DBHandle db, final int neededFieldWidth) throws SQLException {
		this.adapterA = adapterA;
		this.adapterB = adapterB;
		final Random rand = new Random();
		this.tableName = "DBRelnTBL" + Math.abs(rand.nextLong()) + "" + Math.abs(rand.nextLong()); // assume it is a unique name, just for demo
		stmt = db.conn.createStatement();
		stmt.executeUpdate("CREATE TEMPORARY TABLE " + tableName + " (A VARCHAR(" + neededFieldWidth + ") NOT NULL, B VARCHAR(" + neededFieldWidth + "));");
		stmt.executeUpdate("CREATE UNIQUE INDEX " + tableName + "_index ON " + tableName + "(a,b)");
		insertStmt = db.conn.prepareStatement("INSERT INTO " + tableName + " (A,B) VALUES (?, ?)");
		deleteStmt = db.conn.prepareStatement("DELETE FROM " + tableName + " WHERE A=? AND B=?");
	}

	@Override
	public void insertReln(A a, B b) throws IOException {
		try {
			deleteStmt.setString(1,adapterA.toString(a));
			deleteStmt.setString(2,adapterB.toString(b));
			deleteStmt.executeUpdate();
			insertStmt.setString(1,adapterA.toString(a));
			insertStmt.setString(2,adapterB.toString(b));
			insertStmt.executeUpdate();
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}
	

	/**
	 * Not thread safe (shared SQL statement accross instances)
	 */
	@Override
	public Iterable<Entry<A, Iterable<B>>> entries() throws IOException,
			InterruptedException {
		final Iterable<BurstMap> source = new DBIterable(stmt,"SELECT A,B from " + tableName + " GROUP BY A,B ORDER BY A,B");
		final Iterable<Map.Entry<A,B>> lines = new Iterable<Map.Entry<A,B>>() {
			@Override
			public Iterator<Entry<A,B>> iterator() {
				return new Iterator<Entry<A,B>>() {
					final Iterator<BurstMap> underlying = source.iterator(); 
					@Override
					public boolean hasNext() {
						return underlying.hasNext();
					}

					@Override
					public Entry<A,B> next() {
						if(!hasNext()) {
							throw new NoSuchElementException();
						}
						final BurstMap mp = underlying.next();
						final String astr = mp.getAsString("A");
						final String bstr = mp.getAsString("B");
						return new ME<A,B>(adapterA.parse(astr),adapterB.parse(bstr));
					}

					@Override
					public void remove() {
						throw new UnsupportedOperationException();
					}
				};
			}
		};
		return new GroupingIterable<A,B>(adapterA,adapterB,lines);
	}

	@Override
	public void close() throws IOException {
		try {
			stmt.executeUpdate("DROP TABLE " + tableName);
			stmt.close();
			insertStmt.close();
			stmt = null;
			insertStmt = null;
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}
}
