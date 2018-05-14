package lab4;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * My custom partitioner
 * 
 * @author drewu
 *
 */
public class MyPartitioner extends Configured implements Tool
{
	/**
	 * Main method will execute the run method of our
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new MyPartitioner(), args);
		if (args.length != 2)
		{
			System.err.println("Usage: MyPartitioner <in> <output>");
			System.exit(2);
		}
		System.exit(res);

	}

	/**
	 * Run method to run our job
	 * 
	 * @param arg
	 *            Arguments passed from command line
	 * 
	 * @return Whether or not we were successful
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 * @throws Exception
	 */
	@Override
	public int run(String[] arg) throws IOException, ClassNotFoundException, InterruptedException
	{
		Job job = Job.getInstance(new Configuration(), "MyPartitioner");
		job.setJarByClass(MyPartitioner.class);

		// Use arguments passed for input/output paths
		FileInputFormat.setInputPaths(job, new Path(arg[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg[1]));

		// Set our mapper class as well as its respective outputs
		job.setMapperClass(MapPartition.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// Assign our custom partitioner class
		job.setPartitionerClass(KeyPartitioner.class);

		// Set up our number of reducers, as well as its Key:Value output
		job.setReducerClass(ReducePartition.class);
		job.setNumReduceTasks(10);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Wait for job to run
		System.exit(job.waitForCompletion(true) ? 0 : 1);

		return 0;
	}

	/**
	 * Custom Mapper class for partitioner
	 * 
	 * @author drewu
	 *
	 */
	public static class MapPartition extends Mapper<LongWritable, Text, Text, Text>
	{
		/**
		 * Private text variable for key input
		 */
		private Text	currentKey		= new Text();

		/**
		 * Private text variable for value input
		 */
		private Text	currentValue	= new Text();

		/**
		 * Map method to assign our Key:Value pairing from textx file
		 * 
		 * @param key
		 *            Line number, should be ignored
		 * @param value
		 *            Key:Value pairing is stored here
		 * @param context
		 *            Used for emitting our tuple
		 */
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			StringTokenizer s = new StringTokenizer(value.toString());
			currentKey = new Text(s.nextToken());
			currentValue = new Text(s.nextToken());
			context.write(currentKey, currentValue);
		}
	}

	/**
	 * Custom reducer for our paritioner
	 * 
	 * @author drewu
	 *
	 */
	public static class ReducePartition extends Reducer<Text, Text, Text, Text>
	{
		/**
		 * Pretty simple, just emit our key:value pairing
		 * 
		 * @param key
		 * @param value
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public void reduce(Text key, Text value, Context context) throws IOException, InterruptedException
		{
			context.write(key, value);
		}
	}

	/**
	 * The decisions for our partitions
	 * 
	 * Although this can be exploited with unevenly distributed data sets, this
	 * partition relies on an equally distributed set of alphanumeric ordering
	 * throughout the set. Then proceeds to organize the set with regards to
	 * these partitions
	 * 
	 * @author drewu
	 *
	 */
	public static class KeyPartitioner extends Partitioner<Text, Text>
	{

		/**
		 * The getPartition method will divide into 10 partitions among the 62
		 * available alphanumeric characters.
		 */
		@Override
		public int getPartition(Text key, Text value, int numPartitions)
		{
			String thisKey = key.toString();
			if (numPartitions == 0)
				return 0;

			if ("0123456".indexOf(thisKey.charAt(0)) >= 0)
				return 0;

			if ("789ABCD".indexOf(thisKey.charAt(0)) >= 0)
				return 1;

			if ("EFGHIJ".indexOf(thisKey.charAt(0)) >= 0)
				return 2;

			if ("KLMNOP".indexOf(thisKey.charAt(0)) >= 0)
				return 3;

			if ("QRSTUV".indexOf(thisKey.charAt(0)) >= 0)
				return 4;

			if ("WXYZab".indexOf(thisKey.charAt(0)) >= 0)
				return 5;

			if ("cdefgh".indexOf(thisKey.charAt(0)) >= 0)
				return 6;

			if ("ijklmn".indexOf(thisKey.charAt(0)) >= 0)
				return 7;

			if ("opqrst".indexOf(thisKey.charAt(0)) >= 0)
				return 8;

			if ("uvwxyz".indexOf(thisKey.charAt(0)) >= 0)
				return 9;

			return 0;
		}

	}

}
