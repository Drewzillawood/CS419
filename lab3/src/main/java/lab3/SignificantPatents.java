package lab3;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This class detects the strongest connected patents
 * 
 * @author drewu
 *
 */
public class SignificantPatents
{

	/**
	 * Main method will execute all of our jobs in one go
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{

		String input = "/user/drewu/lab3/testing.txt";
		String output1 = "/user/drewu/lab3/exp1/output/";
		// String output2 = "/user/drewu/lab3/exp1/output2/";

		/*
		 * Deciding on a set of 4 reduce tasks initially Preliminary task will
		 * be to discover relations
		 */
		int reduce_tasks = 4;

		ArrayList<SetJob<?, ?>> jobs = new ArrayList<SetJob<?, ?>>();
		jobs.add(new SetJob<MapDiscover, ReduceDiscover>("Round One - Discovery", input, output1, reduce_tasks,
				MapDiscover.class, ReduceDiscover.class));
		// jobs.add(new SetJob<MapLink, ReduceLink>("Round two - Linking",
		// output1, output2, reduce_tasks, MapLink.class,
		// ReduceLink.class));

		for (SetJob<?, ?> j : jobs)
		{
			j.initiate();
		}

	}

	/**
	 * Organize every pair of Patents from our registered input into respective
	 * SortedMapWritable objects.
	 * 
	 * @author drewu
	 *
	 */
	public static class MapDiscover extends Mapper<LongWritable, Text, IntWritable, SortedMapWritable>
	{
		/**
		 * This Job will place every initial pair of patents into
		 * SorteMapWritables, these will be their VALUE
		 */
		SortedMapWritable sortedMap = new SortedMapWritable();

		/**
		 * Input is received line by line
		 * 
		 * @param key
		 *            The first number is going to be the line number and can be
		 *            ignored
		 * @param value
		 *            The value will be the current line of text
		 */
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			StringTokenizer tokens = new StringTokenizer(line);

			// Retrieve the two patentID numbers and add them into our map
			while (tokens.hasMoreTokens())
			{
				IntWritable patentID = new IntWritable(Integer.parseInt(tokens.nextToken()));
				sortedMap.put(new IntWritable(patentID.hashCode()), patentID);
			}
			context.write((IntWritable)sortedMap.firstKey(), sortedMap);
		}
	}

	/**
	 * Preliminary reducer class, all duplicates will be reduced into singles,
	 * as we do not care for any multiples.
	 * 
	 * @author drewu
	 *
	 */
	public static class ReduceDiscover extends Reducer<IntWritable, SortedMapWritable, IntWritable, Text>
	{
		@Override
		public void reduce(IntWritable key, Iterable<SortedMapWritable> values, Context context)
				throws IOException, InterruptedException, FileNotFoundException
		{
			for (SortedMapWritable w : values)
			{
				for (SortedMapWritable s : values)
				{
					if (w.lastKey().equals(s.firstKey()) && !s.lastKey().equals(w.firstKey())
							&& !w.containsKey(s.lastKey()))
					{
						w.put(s.lastKey(), s.get(s.lastKey()));
						break;
					}
				}
				context.write(key, new Text(w.values().toString()));
			}
		}
	}

	/**
	 * Will attempt to link the next set of maps together
	 * 
	 * @author drewu
	 *
	 */
	public static class MapLink extends Mapper<LongWritable, SortedMapWritable, LongWritable, SortedMapWritable>
	{
		@Override
		public void map(LongWritable key, SortedMapWritable value, Context context)
				throws IOException, InterruptedException
		{
			context.write(key, value);
		}
	}

	public static class ReduceLink extends Reducer<LongWritable, SortedMapWritable, LongWritable, SortedMapWritable>
	{
		public void reduce(LongWritable key, SortedMapWritable value, Context context)
				throws IOException, InterruptedException
		{
			context.write(key, value);
		}
	}
}
