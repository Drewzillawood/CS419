package lab5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.LineReader;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

/**
 * My JSON Custom input format class
 * 
 * @author drewu
 *
 */
public class JSONRecordParser
{
	/**
	 * Our big ol wonderful main method
	 * 
	 * @param args
	 * 
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		// Typical job setup
		int reduce_tasks = 4;
		String temp = "/user/drewu/lab5/exp1/temp";

		ArrayList<SetJob<?, ?, ?>> jobs = new ArrayList<SetJob<?, ?, ?>>();
		jobs.add(new SetJob<JSONMapper, JSONReducer, JSONInputFormat>("Parse JSON Input", args[0], temp, reduce_tasks,
				JSONMapper.class, JSONReducer.class, JSONInputFormat.class));
		jobs.add(new SetJob<MaxMapper, MaxReducer, TextInputFormat>("Sort our values", temp, args[1], 1,
				MaxMapper.class, MaxReducer.class, TextInputFormat.class));

		for (SetJob<?, ?, ?> j : jobs)
			j.initiate();
	}

	/**
	 * Generic mapper class
	 * 
	 * @author drewu
	 *
	 */
	public static class JSONMapper extends Mapper<LongWritable, Text, Text, LongWritable>
	{
		/**
		 * Assign each hashtag a constant count as they go out
		 */
		private final static LongWritable	one			= new LongWritable(1);

		/**
		 * Private JSON Object to store the resulting parsed JSON object
		 */
		private JsonObject					tweet		= new JsonObject();

		/**
		 * We will retrieve the 'entities' from the tweet
		 */
		private JsonObject					entities	= new JsonObject();

		/**
		 * From the entities we can retrieve the hashtag
		 */
		private JsonArray					hashtags	= new JsonArray();

		/**
		 * Simple map method for now
		 * 
		 * @param key
		 *            Will just be line number, can ignore
		 * @param value
		 *            Text on current line
		 * @param context
		 *            Context of this mapper
		 * 
		 * @throws IOException
		 * @throws InterruptedException
		 * 
		 */
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			try
			{
				boolean writable = false;
				// Parse our JSON object into an individual Tweet
				tweet = (JsonObject) new JsonParser().parse(value.toString());
				// Grab the object labeled entities
				if (tweet.has("entities"))
				{
					entities = tweet.getAsJsonObject("entities");
					if (entities.has("hashtags"))
					{
						// The hashtags are stored here in an array of anonymous
						// objects
						hashtags = entities.getAsJsonArray("hashtags");
						writable = true;
					}
				}

				// Go through to each hashtag that is within our object
				if (writable)
					for (JsonElement j : hashtags)
						context.write(new Text(j.getAsJsonObject().get("text").toString()), one);
			}
			catch (JsonSyntaxException j)
			{
				j.printStackTrace();
			}
		}
	}

	/**
	 * Generic reducer class
	 * 
	 * @author drewu
	 *
	 */
	public static class JSONReducer extends Reducer<Text, LongWritable, Text, LongWritable>
	{

		// /**
		// * Count to help manage memory
		// */
		// private int count = 0;

		/**
		 * Simple reduce method for now
		 * 
		 * @param key
		 *            Line number
		 * @param values
		 *            Text values we are receiving
		 * @param context
		 *            Context of this reducer
		 * 
		 * @throws IOException
		 * @throws InterruptedException
		 * 
		 */
		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException
		{
			int sum = 0;
			for (LongWritable val : values)
				sum += val.get();

			context.write(key, new LongWritable(sum));
		}
	}

	/**
	 * This is basically just going to sort my max results for me
	 * 
	 * @author drewu
	 *
	 */
	public static class MaxMapper extends Mapper<LongWritable, Text, LongWritable, Text>
	{
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			StringTokenizer s = new StringTokenizer(value.toString());
			Text newVal = new Text(s.nextToken());
			LongWritable newKey = new LongWritable(Long.parseLong(s.nextToken()));
			context.write(newKey, newVal);
		}
	}

	/**
	 * This is basically just going to sort my max results for me
	 * 
	 * @author drewu
	 *
	 */
	public static class MaxReducer extends Reducer<LongWritable, Text, LongWritable, Text>
	{
		public void reduce(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			context.write(key, value);
		}
	}

	/**
	 * Implements our custom record reader
	 * 
	 * @author drewu
	 *
	 */
	public static class JSONInputFormat extends FileInputFormat<LongWritable, Text>
	{
		@Override
		public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException
		{
			return new CustomJSONRecordReader();
		}
	}

	/**
	 * Actual JSON reading occurs within this class
	 * 
	 * @author drewu
	 *
	 */
	public static class CustomJSONRecordReader extends RecordReader<LongWritable, Text>
	{
		/**
		 * Will read our input line by line
		 */
		private LineReader		lineReader;

		/**
		 * Key of our current [key : value] pair
		 */
		private LongWritable	key;

		/**
		 * Value of our current [key : value] pair
		 */
		private Text			value;

		/**
		 * The starting point of our file
		 */
		private long			start;

		/**
		 * The ending point of our file
		 */
		private long			end;

		/**
		 * Where we are currently within our file
		 */
		private long			position;

		/**
		 * How many calls to nextKeyValue we have made
		 */
		private long			noOfCalls;

		/**
		 * Initialize our parser
		 * 
		 * @param genericSplit
		 *            regular split
		 * @param context
		 *            context for this initialization
		 * 
		 * @throws IOException
		 */
		@Override
		public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException
		{
			Configuration conf = context.getConfiguration();

			// General file declarations and initializations
			FileSplit split = (FileSplit) genericSplit;
			final Path file = split.getPath();
			FileSystem fs = file.getFileSystem(conf);

			// Position initialization
			start = split.getStart();
			end = start + split.getLength();
			position = start;

			// Opening our inputFile
			FSDataInputStream inputFile = fs.open(split.getPath());
			inputFile.seek(start);

			// Initialize our lineReader
			lineReader = new LineReader(inputFile, conf);

			Text temp = new Text(" ");
			while (temp.charAt(0) != '{' && position < end)
			{
				int readLength = lineReader.readLine(temp);
				position += readLength;
			}

			noOfCalls = 0;
		}

		/**
		 * Returns the current progress of the job
		 * 
		 * @return
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Override
		public float getProgress() throws IOException, InterruptedException
		{
			if (start == end)
				return 0.0f;
			else
				return Math.min(1.0f, (position - start) / (float) (end - start));
		}

		/**
		 * Show us our current key
		 * 
		 * @return The current key being operated on
		 * 
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException
		{
			return key;
		}

		/**
		 * Show us our current value
		 * 
		 * @return The current value being operated on
		 * 
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Override
		public Text getCurrentValue() throws IOException, InterruptedException
		{
			return value;
		}

		/**
		 * Lets us know if there are any more records to read
		 * 
		 * @return True if there are still splits to be read
		 * 
		 * @throws IOException
		 */
		@Override
		public boolean nextKeyValue() throws IOException
		{
			// Edge case and error prevention
			if (position >= end)
				return false;
			if (key == null)
				key = new LongWritable();
			if (value == null)
				value = new Text(" ");

			// Increment our calls to nextKeyValue
			noOfCalls++;
			key.set(noOfCalls);

			// lineReader will walk the length of our split
			Text t = new Text(" ");
			int readLength = lineReader.readLine(t);
			String line = t.toString();
			position += readLength;

			// Starting with first bracket of JSON object
			String JSONString = "{ ";
			// int bracketCount = 1;

			// Start appending values to our JSON object until we find the
			// matching end bracket
			while (position < end /* && bracketCount > 0 */)
			{
				readLength = lineReader.readLine(t);
				line = t.toString();
				position += readLength;

				// Need to keep track of internal objects
				// if (line.contains("{"))
				// bracketCount++;
				// if (line.contains("}"))
				// bracketCount--;

				// Once brackets have cancelled out evenly, we have a
				// complete object
				if (line.charAt(0) == '}')
				{
					JSONString += "}";
					value.set(JSONString);
					return true;
				}

				JSONString += line + " ";
			}

			return false;
		}

		/**
		 * Close our current lineReader
		 * 
		 * @throws IOException
		 */
		@Override
		public void close() throws IOException
		{
			if (lineReader != null)
				lineReader.close();
		}
	}
}
