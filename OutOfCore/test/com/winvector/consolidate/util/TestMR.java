package com.winvector.consolidate.util;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.junit.Test;

import com.winvector.consolidate.def.DataAdapter;
import com.winvector.consolidate.impl.FileListIterable;
import com.winvector.consolidate.impl.GroupingIterable;
import com.winvector.consolidate.impl.MapReduceRelnCollector;





public class TestMR {

	@Test
	public void testMRsetup() throws IOException {
		final String[][] dat = {
				{"a", "a"},
				{"b", "a"},
				{"a", "b"},
				{"a", "a"},
		};
		final Map<String, Set<String>> expect = new TreeMap<String,Set<String>>();
		expect.put("a",TestGrouping.set(new String[] {"a","b"}));
		expect.put("b",TestGrouping.set(new String[] {"a"}));
		final File dir = File.createTempFile("MRWork",".dir");
		//System.out.println("working in: " + dir.getAbsolutePath());
		dir.delete();
		dir.mkdir();
		final File input = File.createTempFile("MRInput", ".txt", dir);
		final File res = File.createTempFile("MROutput", ".txt", dir);
		res.delete();
		{
			final PrintStream p = new PrintStream(new FileOutputStream(input));
			for(final String[] row: dat) {
				p.println(row[0] + "\t" + row[1]);
			}
			p.close();
		}
		//input.deleteOnExit();
		//dir.deleteOnExit();
		final JobConf conf = new JobConf(TestMR.class);
		conf.setJobName("testmr");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(MapReduceRelnCollector.MapS.class);
		conf.setCombinerClass(MapReduceRelnCollector.ReduceS.class);
		conf.setReducerClass(MapReduceRelnCollector.ReduceS.class);
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
				//f.deleteOnExit();
			}
		}
		final DataAdapter<String> trivialAdapter = new DataAdapter<String>() {
			@Override
			public int compare(String arg0, String arg1) {
				return arg0.compareTo(arg1);
			}

			/**
			 * not a reversible encoding with respect to white space
			 * @param k
			 * @return
			 */
			@Override
			public String toString(String k) {
				return k.replaceAll("\\s+"," ").trim();
			}

			@Override
			public String parse(String s) {
				return s;
			}
		};
		final Iterable<Map.Entry<String,Iterable<String>>> results = new GroupingIterable<String,String>(trivialAdapter,trivialAdapter,new FileListIterable<String,String>(trivialAdapter,trivialAdapter,sources));
		//for(final Map.Entry<String, Set<String>> row: results) {
		//	System.out.println(row.getKey() + "\t->\t" + row.getValue());
		//}
		final boolean eq = TestGrouping.equals(expect, results);
		// clean up
		for(final File f: res.listFiles()) {
			f.delete();
		}
		res.delete();
		input.delete();
		dir.delete();
		assertTrue(eq);
	}
}
