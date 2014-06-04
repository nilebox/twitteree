package ru.nilebox.twitteree.consumer;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

/**
 *
 * @author
 * nile
 */
public class StatusHandler implements StatusListener {

	public static final String TABLE_NAME = "twitteree";
	public static final String FAMILY_NAME = "text";
	public static final String QUALIFIER_NAME = "contents";
	private Configuration config;
	
	public StatusHandler() {
		try {
			init();
		} catch (Exception ex) {
			System.out.println(ex.toString());
		}
	}

	public void onStatus(Status status) {
		if (!status.getLang().equals("en")) {
			return;
		}

//		CompactLanguageDetector compactLanguageDetector = new CompactLanguageDetector();
//		LanguageDetectionResult result = compactLanguageDetector.detect(status.getText());
//		if (result.isReliable()) {
//			// getProbableLocale returns a java.util.Locale
//			System.out.println("Pretty sure that's " + result.getProbableLocale().getLanguage() + " " + result.getProbableLocale().getDisplayName());
//		} else {
//			for (LanguageDetectionCandidate candidate : result.getCandidates()) {
//				System.out.println("Maybe it's " + result.getProbableLocale().getLanguage() + " " + candidate.getLocale().getDisplayName());
//			}
//		}
//
//		if (!result.getProbableLocale().getLanguage().equals("en")) {
//			return;
//		}

		System.out.println("Twitter message: " + status.getText());
		try {
			storeMessage(status);
		} catch (IOException ex) {
			System.out.println(ex.toString());
		}
	}

	public void onDeletionNotice(StatusDeletionNotice sdn) {
	}

	public void onTrackLimitationNotice(int i) {
	}

	public void onScrubGeo(long l, long l1) {
	}

	public void onStallWarning(StallWarning sw) {
	}

	public void onException(Exception excptn) {
	}
	
	private void storeMessage(Status status) throws IOException {
		// This instantiates an HTable object that connects you to
		// the "myLittleHBaseTable" table.
		HTable table = new HTable(config, TABLE_NAME);

		// To add to a row, use Put.  A Put constructor takes the name of the row
		// you want to insert into as a byte array.  In HBase, the Bytes class has
		// utility for converting all kinds of java types to byte arrays.  In the
		// below, we are converting the String "myLittleRow" into a byte array to
		// use as a row key for our update. Once you have a Put instance, you can
		// adorn it by setting the names of columns you want to update on the row,
		// the timestamp to use in your update, etc.If no timestamp, the server
		// applies current time to the edits.
		Put p = new Put(Bytes.toBytes(status.getId()));
		
		// To set the value you'd like to update in the row 'myLittleRow', specify
		// the column family, column qualifier, and value of the table cell you'd
		// like to update.  The column family must already exist in your table
		// schema.  The qualifier can be anything.  All must be specified as byte
		// arrays as hbase is all about byte arrays.  Lets pretend the table
		// 'myLittleHBaseTable' was created with a family 'myLittleFamily'.
		p.add(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(QUALIFIER_NAME),
				Bytes.toBytes(status.getText()));

		// Once you've adorned your Put instance with all the updates you want to
		// make, to commit it do the following (The HTable#put method takes the
		// Put instance you've been building and pushes the changes you made into
		// hbase)
		table.put(p);
	}

	private void init() throws IOException {
		// You need a configuration object to tell the client where to connect.
		// When you create a HBaseConfiguration, it reads in whatever you've set
		// into your hbase-site.xml and in hbase-default.xml, as long as these can
		// be found on the CLASSPATH
		config = HBaseConfiguration.create();

		HBaseAdmin hbase = new HBaseAdmin(config);
		if (!hbase.tableExists(TABLE_NAME)) {
			HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
			HColumnDescriptor textColumn = new HColumnDescriptor(FAMILY_NAME.getBytes());
			desc.addFamily(textColumn);
			hbase.createTable(desc);
		}
	}
	
	private void test() throws IOException {
		// This instantiates an HTable object that connects you to
		// the "myLittleHBaseTable" table.
		HTable table = new HTable(config, "myLittleHBaseTable");

		// To add to a row, use Put.  A Put constructor takes the name of the row
		// you want to insert into as a byte array.  In HBase, the Bytes class has
		// utility for converting all kinds of java types to byte arrays.  In the
		// below, we are converting the String "myLittleRow" into a byte array to
		// use as a row key for our update. Once you have a Put instance, you can
		// adorn it by setting the names of columns you want to update on the row,
		// the timestamp to use in your update, etc.If no timestamp, the server
		// applies current time to the edits.
		Put p = new Put(Bytes.toBytes("myLittleRow"));

		// To set the value you'd like to update in the row 'myLittleRow', specify
		// the column family, column qualifier, and value of the table cell you'd
		// like to update.  The column family must already exist in your table
		// schema.  The qualifier can be anything.  All must be specified as byte
		// arrays as hbase is all about byte arrays.  Lets pretend the table
		// 'myLittleHBaseTable' was created with a family 'myLittleFamily'.
		p.add(Bytes.toBytes("myLittleFamily"), Bytes.toBytes("someQualifier"),
				Bytes.toBytes("Some Value"));

		// Once you've adorned your Put instance with all the updates you want to
		// make, to commit it do the following (The HTable#put method takes the
		// Put instance you've been building and pushes the changes you made into
		// hbase)
		table.put(p);

		// Now, to retrieve the data we just wrote. The values that come back are
		// Result instances. Generally, a Result is an object that will package up
		// the hbase return into the form you find most palatable.
		Get g = new Get(Bytes.toBytes("myLittleRow"));
		Result r = table.get(g);
		byte[] value = r.getValue(Bytes.toBytes("myLittleFamily"),
				Bytes.toBytes("someQualifier"));
		// If we convert the value bytes, we should get back 'Some Value', the
		// value we inserted at this location.
		String valueStr = Bytes.toString(value);
		System.out.println("GET: " + valueStr);

		// Sometimes, you won't know the row you're looking for. In this case, you
		// use a Scanner. This will give you cursor-like interface to the contents
		// of the table.  To set up a Scanner, do like you did above making a Put
		// and a Get, create a Scan.  Adorn it with column names, etc.
		Scan s = new Scan();
		s.addColumn(Bytes.toBytes("myLittleFamily"), Bytes.toBytes("someQualifier"));
		ResultScanner scanner = table.getScanner(s);
		try {
			// Scanners return Result instances.
			// Now, for the actual iteration. One way is to use a while loop like so:
			for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
				// print out the row we found and the columns we were looking for
				System.out.println("Found row: " + rr);
			}

			// The other approach is to use a foreach loop. Scanners are iterable!
			// for (Result rr : scanner) {
			//   System.out.println("Found row: " + rr);
			// }
		} finally {
			// Make sure you close your scanners when you are done!
			// Thats why we have it inside a try/finally clause
			scanner.close();
		}
	}
}
