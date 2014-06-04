package ru.nilebox.twitteree.analytics.hashtags.hbase;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 *
 * @author
 * nile
 */
public class HashtagTableMapper extends TableMapper<Text, IntWritable>  {
	public static final byte[] FAMILY = "text".getBytes();
	public static final byte[] QUALIFIER = "contents".getBytes();

	private final IntWritable ONE = new IntWritable(1);

	@Override
   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
		String text = new String(value.getValue(FAMILY, QUALIFIER));
		char[] chars = text.toCharArray();
		
		int startIndex = -1;
		for (int i=0; i<chars.length; i++) {
			if (startIndex >= 0) {
				// we met hashtag end, store it
				if (chars[i] == '#'
						|| !(chars[i] == '_'
						|| (chars[i] >= 'a' && chars[i] <= 'z')
						|| (chars[i] >= 'A' && chars[i] <= 'Z')
						|| (chars[i] >= '0' && chars[i] <= '9'))) {
					writeHashtag(text.substring(startIndex, i), context);
					startIndex = -1;
				}
			} else if (chars[i] == '#') {
				startIndex = i+1;
			}
		}
		
		// store last hashtag
		if (startIndex >= 0)
			writeHashtag(text.substring(startIndex), context);
   	}
	
	private void writeHashtag(String hashtag, Context context) throws IOException, InterruptedException {
		// write to mapper output
		context.write(new Text(hashtag.trim().toLowerCase()), ONE);
	}
}
