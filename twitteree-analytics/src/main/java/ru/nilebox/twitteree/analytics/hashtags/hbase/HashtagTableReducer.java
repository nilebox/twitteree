package ru.nilebox.twitteree.analytics.hashtags.hbase;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 *
 * @author
 * nile
 */
public class HashtagTableReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable>  {
	public static final byte[] FAMILY = "hashtags".getBytes();
	public static final byte[] QUALIFIER = "count".getBytes();

	@Override
 	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    		int i = 0;
    		for (IntWritable val : values) {
    			i += val.get();
    		}
			String rowKey = String.format("%06d", 1000000 - i) + "#" + key;
    		Put put = new Put(Bytes.toBytes(rowKey));
    		put.add(FAMILY, QUALIFIER, Bytes.toBytes(i));

    		context.write(null, put);
   	}
}
