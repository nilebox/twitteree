package ru.nilebox.twitteree.analytics;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import ru.nilebox.twitteree.analytics.hashtags.file.HashtagMapper;
import ru.nilebox.twitteree.analytics.hashtags.file.HashtagReducer;
import ru.nilebox.twitteree.analytics.hashtags.hbase.HashtagTableMapper;
import ru.nilebox.twitteree.analytics.hashtags.hbase.HashtagTableReducer;

/**
 * Hello world!
 *
 */
public class App 
{
	private static final String SOURCE_TABLE = "twitteree";
	private static final String TARGET_TABLE = "analytics";
	
    public static void main( String[] args ) throws Exception
    {
        System.out.println( "Starting hadoop task" );
		
		Configuration config = getConfiguration();
		initTargetTable(config);
		
		Scan scan = getScan();
		
		Job job = new Job(config, "Twitter hashtags counter");
		job.setJarByClass(App.class);
		
		TableMapReduceUtil.initTableMapperJob(
				SOURCE_TABLE, // input table
				scan, // Scan instance to control CF and attribute selection
				HashtagTableMapper.class, // mapper class
				Text.class,         // mapper output key
				IntWritable.class,  // mapper output value
				job);
		TableMapReduceUtil.initTableReducerJob(
				TARGET_TABLE, // output table
				HashtagTableReducer.class, // reducer class
				job);
		
//		FileInputFormat.addInputPath(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//		job.setMapperClass(HashtagMapper.class);
//		job.setReducerClass(HashtagReducer.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(1);   // at least one, adjust as required
		
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
		
		System.out.println( "Hadoop task finished" );
		
		System.exit(b ? 0 : 1);
    }
	
	private static Configuration getConfiguration() {
		Configuration config = HBaseConfiguration.create();
		return config;
	}
	
	private static Scan getScan() {
		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		return scan;
	}
	
	private static void initTargetTable(Configuration config) throws Exception {
		HBaseAdmin hbase = new HBaseAdmin(config);
		if (!hbase.tableExists(TARGET_TABLE)) {
			HTableDescriptor desc = new HTableDescriptor(TARGET_TABLE);
			HColumnDescriptor textColumn = new HColumnDescriptor(HashtagTableReducer.FAMILY);
			desc.addFamily(textColumn);
			hbase.createTable(desc);
		}
	}
}
