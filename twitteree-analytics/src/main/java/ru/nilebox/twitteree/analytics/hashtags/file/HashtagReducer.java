package ru.nilebox.twitteree.analytics.hashtags.file;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author
 * nile
 */
public class HashtagReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		Iterator<IntWritable> it = values.iterator();
		while (it.hasNext()) {
			IntWritable value = it.next();
			count += value.get();
		}
		context.write(key, new IntWritable(count));
	}
	
}
