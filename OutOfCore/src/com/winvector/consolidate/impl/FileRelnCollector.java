package com.winvector.consolidate.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import com.winvector.consolidate.def.DataAdapter;
import com.winvector.consolidate.def.RelnCollector;

public final class FileRelnCollector<A,B> implements RelnCollector<A,B> {
	private final char sep = '\t';
	private final DataAdapter<A> adapterA;
	private final DataAdapter<B> adapterB;
	private final String sortBinPath;
	private final File backing;
	private final ArrayList<File> sorted = new ArrayList<File>();
	private PrintStream p;
	
	public FileRelnCollector(final DataAdapter<A> adapterA, final DataAdapter<B> adapterB, final String sortBinPath) throws IOException {
		this.adapterA = adapterA;
		this.adapterB = adapterB;
		this.sortBinPath = sortBinPath;
		backing = File.createTempFile("FileRelnCollectorCollector",".txt");
		backing.deleteOnExit();
		sorted.add(File.createTempFile("FileRelnCollectorSorted",".txt"));
		for(final File f: sorted) {
			f.deleteOnExit();
		}
		p = new PrintStream(new FileOutputStream(backing));
	}

	@Override
	public void insertReln(final A a, final B b) {
		p.println(adapterA.toString(a) + sep + adapterB.toString(b));
	}
	
	

	@Override
	public Iterable<Entry<A,Iterable<B>>> entries() throws IOException, InterruptedException {
		if(null!=p) {
			p.close();
			p = null;
		}
		final Runtime runtime = Runtime.getRuntime();
		final Process proc = runtime.exec(new String[] {sortBinPath, "-u", "-o", sorted.get(0).getAbsolutePath(), backing.getAbsolutePath()});
		final int status = proc.waitFor(); // expect status 0
		if(status!=0) {
			throw new IOException("FileRelnCollector sort status returned: " + status);
		}
		final Iterable<Map.Entry<A,B>> lines = new FileListIterable<A,B>(adapterA,adapterB,sorted);
		//for(final Entry<A, B> me: lines) {
		//	System.out.println("line:\t" + me.getKey() + "\t" + me.getValue());
		//}
		final Iterable<Map.Entry<A,Iterable<B>>> groups = new GroupingIterable<A,B>(adapterA,adapterB,lines); 
		//for(final Entry<A,Set<B>> me: groups) {
		//	System.out.println("group:\t" + me.getKey() + "\t" + me.getValue());
		//}
		return groups;
	}
	
	@Override
	public void close() {
		if(null!=p) {
			p.close();
			p = null;
		}
		backing.delete();
		for(final File f: sorted) {
			f.delete();
		}
	}
}
