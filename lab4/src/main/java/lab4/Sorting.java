package lab4;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

/**
 * Simple sorting algorithm using MapReduce
 * 
 * @author drewu
 *
 */
public class Sorting
{
	/**
	 * Typical main method for job execution
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		// Series of input and output files
		String input = "/cpre419/input-50m";
		String partitionerOutput = "/user/drewu/lab4/exp1/partitionerOutput/";
		String output = "/user/drewu/lab4/exp1/output/";

		Job job = Job.getInstance(new Configuration(), "Total Order Sorting Example");
		job.setJarByClass(Sorting.class);

		// Max of 10 reducer tasks is allowed
		job.setNumReduceTasks(10);
		FileInputFormat.setInputPaths(job, new Path(input));

		// Using TotalOrderPartitioner for Global sorting
		TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path(partitionerOutput));

		// Taking advantage of Key:Value pairing in text format
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// Using RandomSampler with 10% change any key could be selected, sample
		// size of 10, and 9 splits
		InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<>(10, 10, 9);
		InputSampler.writePartitionFile(job, sampler);

		// Set our default mapper and use custom reducer class
		job.setPartitionerClass(TotalOrderPartitioner.class);
		job.setMapperClass(Mapper.class);
		job.setReducerClass(Reduce_1.class);

		FileOutputFormat.setOutputPath(job, new Path(output));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	/**
	 * Custom reducer task
	 * 
	 * @author drewu
	 *
	 */
	public static class Reduce_1 extends Reducer<Text, Text, Text, Text>
	{
		/**
		 * Count to help reset memory with largest dataset
		 */
		private int count = 0;

		/**
		 * Reducer task to write our sorted keys to output
		 */
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			if (count >= 3000)
			{
				System.gc();
				count = 0;
			}
			for (Text t : values)
				context.write(key, t);
			count++;
		}
	}

}
