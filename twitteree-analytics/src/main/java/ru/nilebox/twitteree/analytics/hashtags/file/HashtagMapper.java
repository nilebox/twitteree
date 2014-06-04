package ru.nilebox.twitteree.analytics.hashtags.file;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author
 * nile
 */
public class HashtagMapper extends Mapper<Text, Text, Text, IntWritable> {
	
	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		String text = value.toString();
		char[] chars = text.toCharArray();
		
		int startIndex = -1;
		for (int i=0; i<chars.length; i++) {
			if (startIndex >= 0) {
				// we met hashtag end, store it
				if (chars[i] == '#' || chars[i] == ' ') {
					writeHashtag(text.substring(startIndex, i+1), context);
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
		context.write(new Text(hashtag), new IntWritable(1));
	}
}
