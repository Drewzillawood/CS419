package lab3;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Something to make setting jobs a wee easier
 * 
 * @author drewu
 *
 */
public class SetJob<T extends Mapper<?, ?, ?, ?>, S extends Reducer<?, ?, ?, ?>>
{
	/**
	 * Name of the job
	 */
	private String			name;
	/**
	 * File input path name
	 */
	private String			input;

	/**
	 * File output path name
	 */
	private String			output;

	/**
	 * How many reducer tasks we want for this job
	 */
	private int				reducerTasks;

	/**
	 * Specified Mapper class for this job
	 */
	private final Class<T>	mapper;

	/**
	 * Specified Reducer class for this job
	 */
	private final Class<S>	reducer;

	/**
	 * Set everything up into a compact jobject... heh. This is a more or less
	 * 'default' constructor to keep everything simple. Default values fall
	 * around no argument for conf, IO Text and File formatting being set to the
	 * same thing we did for the first two labs and setting mapper/reducer
	 * classes to the passed parameters
	 * 
	 * @param jobName
	 *            What we want to name this Jon
	 * @param input
	 *            File path name for the input
	 * @param output
	 *            File path name for the output
	 * @param reducerTasks
	 *            Number of reducer Tasks we want
	 * @param map
	 *            Mapper class to be used for this job
	 * @param reduce
	 *            Reducer class to be used for this job
	 * 
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public SetJob(String jobName, String inputPathName, String outputPathName, int reducerTasks, Class<T> mapperClass,
			Class<S> reducerClass) throws IOException, ClassNotFoundException, InterruptedException
	{
		name = jobName;
		this.input = inputPathName;
		this.output = outputPathName;
		this.reducerTasks = reducerTasks;
		mapper = mapperClass;
		reducer = reducerClass;
	}

	/**
	 * Get the whole job underway with the parameters specified in constructor
	 * 
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	@SuppressWarnings("unchecked")
	public void initiate() throws IOException, ClassNotFoundException, InterruptedException
	{
		/*
		 * Retrieving generic Types for Mapper class
		 */
		ParameterizedType mParams = (ParameterizedType) mapper.getGenericSuperclass();
		Type[] mTypes = mParams.getActualTypeArguments();

		/*
		 * Retrieving generic Types for reducer class
		 */
		ParameterizedType rParams = (ParameterizedType) reducer.getGenericSuperclass();
		Type[] rTypes = rParams.getActualTypeArguments();

		/*
		 * Simple setup
		 */
		Configuration conf = new Configuration();
		Job j = Job.getInstance(conf, name);
		j.setJarByClass(SignificantPatents.class);
		j.setNumReduceTasks(reducerTasks);

		// Setting our map and reduce classes
		j.setMapperClass((Class<? extends Mapper<?, ?, ?, ?>>) Class.forName(mapper.getName()));
		j.setReducerClass((Class<? extends Reducer<?, ?, ?, ?>>) Class.forName(reducer.getName()));

		// Mapper output [key, value]
		j.setMapOutputKeyClass(Class.forName(mTypes[2].getTypeName()));
		j.setMapOutputValueClass(Class.forName(mTypes[3].getTypeName()));

		// Reducer output [key, value]
		j.setOutputKeyClass(Class.forName(rTypes[2].getTypeName()));
		j.setOutputValueClass(Class.forName(rTypes[3].getTypeName()));

		// Decides how text input will be split up
		// In this case we will split the data line by line
		j.setInputFormatClass(TextInputFormat.class);
		j.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(j, new Path(input));
		FileOutputFormat.setOutputPath(j, new Path(output));

		j.waitForCompletion(true);
	}

}