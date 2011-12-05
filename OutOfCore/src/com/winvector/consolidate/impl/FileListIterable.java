package com.winvector.consolidate.impl;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import com.winvector.consolidate.def.DataAdapter;


public final class FileListIterable<A,B> implements Iterable<Entry<A,B>> {
	private final char sep = '\t';
	private final DataAdapter<A> adapterA;
	private final DataAdapter<B> adapterB;
	private final Iterable<File> sources;

	public FileListIterable(final DataAdapter<A> adapterA, final DataAdapter<B> adapterB,
			final Iterable<File> sorted) {
		this.adapterA = adapterA;
		this.adapterB = adapterB;
		this.sources = sorted;
	}
	

	private final class FileListIterator implements Iterator<Entry<A,B>> {
		private LinkedList<File> inputs = new LinkedList<File>();
		private LineNumberReader rdr = null;
		private ME<A,B> nextPair = null;
		
		public FileListIterator() throws IOException {
			for(final File f: sources) {
				inputs.addLast(f);
			}				
			advance();
		}
		
		private void advance() throws IOException {
			nextPair = null;
			while(true) {
				if(rdr!=null) {
					final String line = rdr.readLine();
					if(line!=null) {
						final String[] flds = line.split(""+sep);
						nextPair = new ME<A,B>(adapterA.parse(flds[0]),adapterB.parse(flds[1]));
						return;
					} else {
						rdr = null;
					}
				}
				if(rdr==null) {
					if(!inputs.isEmpty()) {
						rdr = new LineNumberReader(new FileReader(inputs.removeFirst()));
					} else {
						return;  // exhausted
					}
				}
			}
		}
		
		@Override
		public boolean hasNext() {
			return nextPair!=null;
		}

		@Override
		public Entry<A,B> next() {
			if(!hasNext()) {
				throw new NoSuchElementException();
			}
			try {
				final Entry<A,B> ret = nextPair;
				advance();
				return ret;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	@Override
	public Iterator<Entry<A,B>> iterator() {
		try {
			return new FileListIterator();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
