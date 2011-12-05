package com.winvector.consolidate.util;

import static org.junit.Assert.*;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.Statement;

import org.junit.Test;

import com.winvector.consolidate.util.DBUtil.DBHandle;

public class TestDB {
	@Test
	public void testDBPath() throws Exception {
		// example data (in order)
		final String[][] rows = new String[][] {
				{"apple", "cobler"},
				{"apple", "pie"},
				{"pie", "toss"},
		};
		// build DB connection
		final String comment = "test";
		final String dbUserName = "";
		final String dbPassword = "";
		final String driver = "org.h2.Driver";
		final File tmpFile = File.createTempFile("TestH2DB",".dir");
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
		final Statement stmt = handle.conn.createStatement();
		// copy data into table
		stmt.executeUpdate("CREATE TABLE tmptable (a VARCHAR(30) NOT NULL, b VARCHAR(30));");
		{
			final PreparedStatement insertStmt = handle.conn.prepareStatement("INSERT INTO tmptable (a,b) VALUES (?, ?)");
			for(final String[] row: rows) {
				for(int i=0;i<row.length;++i) {
					insertStmt.setString(i+1,row[i]);
				}
				insertStmt.executeUpdate();
			}
			insertStmt.close();
		}
		// bring data back out of table
		final Iterable<BurstMap> source = new DBIterable(stmt,"SELECT a,b FROM tmptable ORDER BY a,b");
		int rownum = 0;
		for(final BurstMap row: source) {
			//System.out.println("got:\t" + row);
			assertEquals(row.getAsString("A"),rows[rownum][0]);
			assertEquals(row.getAsString("B"),rows[rownum][1]);
			++rownum;
		}
		assertEquals(rows.length,rownum);
		stmt.close();
		handle.conn.close();
		// clean up
		for(final File ci: tmpFile.listFiles()) {
			ci.delete();
		}
		tmpFile.delete();
	}
}
