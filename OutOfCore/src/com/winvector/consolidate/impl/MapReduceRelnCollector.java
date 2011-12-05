package com.winvector.consolidate.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.winvector.consolidate.def.DataAdapter;
import com.winvector.consolidate.def.RelnCollector;


public final class MapReduceRelnCollector<A,B> implements RelnCollector<A,B> {
	private final char sep = '\t';
	private final DataAdapter<A> adapterA;
	private final DataAdapter<B> adapterB;
	private final File dir;
	private final File input;
	private final File res;

	private PrintStream p;
	
	public MapReduceRelnCollector(final DataAdapter<A> adapterA, final DataAdapter<B> adapterB) throws IOException {
		this.adapterA = adapterA;
		this.adapterB = adapterB;
		dir = File.createTempFile("MRCol",".dir");
		//System.out.println("working in: " + dir.getAbsolutePath());
		dir.delete();
		dir.mkdir();
		input = File.createTempFile("MRInput", ".txt", dir);
		res = File.createTempFile("MROutput", ".txt", dir);
		res.delete();
		p = new PrintStream(new FileOutputStream(input));
	}

	public static final class MapS extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			final String[] flds = value.toString().split("\t");
			output.collect(new Text(flds[0]),new Text(flds[1]));
		}
	}

	public static final class ReduceS extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			final SortedSet<String> vs = new TreeSet<String>();
			while(values.hasNext()) {
				vs.add(values.next().toString());
			}
			for(final String vi: vs) {
				output.collect(key,new Text(vi));
			}
		}
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
		final JobConf conf = new JobConf(MapReduceRelnCollector.class);
		conf.setJobName("mrcollector");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(MapS.class);
		conf.setCombinerClass(ReduceS.class);
		conf.setReducerClass(ReduceS.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(input.getAbsolutePath()));
		FileOutputFormat.setOutputPath(conf, new Path(res.getAbsolutePath()));
		final RunningJob running = JobClient.runJob(conf);
		running.waitForCompletion();
		//res.deleteOnExit();
		final ArrayList<File> sources = new ArrayList<File>();
		for(final File f: res.listFiles()) {
			if(!f.getName().startsWith("part-")) {
				f.delete();
			} else {
				sources.add(f);
			}
		}
		input.delete();
		final Iterable<Map.Entry<A,B>> lines = new FileListIterable<A,B>(adapterA,adapterB,sources);
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
		for(final File f: res.listFiles()) {
			f.delete();
		}
		res.delete();
		input.delete();
		dir.delete();
	}
}

