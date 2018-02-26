package com.hbase.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class SimpleRowCounterForHBase  {
	static class RowCounterMapper extends TableMapper<ImmutableBytesWritable, Result> {
		public static enum Counters { ROWS }
		@Override
		public void map(ImmutableBytesWritable row, Result value, Context context) {
			context.getCounter(Counters.ROWS).increment(1);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		if (args.length != 1) {
			System.err.println("Usage: SimpleRowCounterForHBase <tablename>");
			return ;
		}
		String tableName = args[0];
		Scan scan = new Scan();
		scan.setFilter(new FirstKeyOnlyFilter());
		Job job = new Job(conf, "RowCount");
		job.setJarByClass(SimpleRowCounterForHBase.class);
		TableMapReduceUtil.initTableMapperJob(tableName, scan,
				RowCounterMapper.class, ImmutableBytesWritable.class, Result.class, job);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(NullOutputFormat.class);
		job.submit();
		
	}
}
