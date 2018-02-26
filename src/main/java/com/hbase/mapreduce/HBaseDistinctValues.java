package com.hbase.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

/*****
 * 
 * 
 * @author Shobha
 * This example counts distinct instances of a value in a table and write those counts in another table
 *
 */

public class HBaseDistinctValues {
	public static void main(String args[]){
		
		Configuration conf = HBaseConfiguration.create();
		Job job;
		String sourceTable="",targetTable ="";
		try {
			if(args.length > 1) {
				sourceTable = args[0];
				targetTable = args[1];
			}
			else {
				System.out.println("Enter table name as argument");
				System.exit(0);
			}
			
			job = new Job(conf, "HBaseDistinctValues");
			job.setJarByClass(HBaseDistinctValues.class);

			Scan scan = new Scan();
			scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
			scan.setCacheBlocks(false);  // don't set to true for MR jobs
			
			TableMapReduceUtil.initTableMapperJob(
					sourceTable,      // input table
					scan,	          // Scan instance to control CF and attribute selection
					MyMapper.class,   // mapper class
					Text.class,	      // mapper output key
					IntWritable.class,// mapper output value
					job);
			
				TableMapReduceUtil.initTableReducerJob(
					targetTable,      // output table
					MyReducer.class,  // reducer class
					job);
				job.setNumReduceTasks(0);

				boolean b = job.waitForCompletion(true);
				if (!b) {
				    throw new IOException("error with job!");
				}
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public static class MyMapper extends TableMapper<Text , IntWritable> {
		public static final byte[] CF = "CF".getBytes();
		public static final byte[] colFam = "colFam".getBytes();
		
		private final IntWritable ONE = new IntWritable(1);
		private Text text = new Text();
		
		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
			String val = new String(value.getValue(CF, colFam));
			text.set(val);
		    context.write(text, ONE);
		}
	}

	public  static class MyReducer extends TableReducer<Text , IntWritable , ImmutableBytesWritable> {
		public static final byte[] CF = "CF".getBytes();
		public static final byte[] colFam = "count".getBytes();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int i = 0;
			for(IntWritable val: values) {
				i += val.get();
			}
			
			Put put = new Put(Bytes.toBytes(key.toString()));
			put.add(CF,colFam,Bytes.toBytes(i));
			
			context.write(null, put);
		}
	}
}
