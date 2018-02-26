package com.hbase.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;

/****
 * 
 * 
 * @author Shobha
 * This HBase map reduce illustrates HBase as source and sink. This example will simply copy data from one table to another
 *
 */
public class HBaseReadWrite {

	public static void main(String args[]) {
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
			
			job = new Job(conf, "HBaseReadWrite");
			job.setJarByClass(HBaseReadWrite.class);

			Scan scan = new Scan();
			scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
			scan.setCacheBlocks(false);  // don't set to true for MR jobs
			
			TableMapReduceUtil.initTableMapperJob(
					sourceTable,      // input table
					scan,	          // Scan instance to control CF and attribute selection
					MyMapper.class,   // mapper class
					null,	          // mapper output key
					null,	          // mapper output value
					job);
			
				TableMapReduceUtil.initTableReducerJob(
					targetTable,      // output table
					null,             // reducer class
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
	
	private static class MyMapper extends TableMapper<ImmutableBytesWritable, Put> {
		public void map(ImmutableBytesWritable row, Result value , Context context) throws IOException, InterruptedException {
			context.write(row, resultToPut(row,value));
		}
	}
	
	private static Put resultToPut(ImmutableBytesWritable key, Result value) throws IOException {
		Put put = new Put(key.get());
		for(KeyValue kv:value.raw()) {
			put.add(kv);
		}
		return put;
	}
}
