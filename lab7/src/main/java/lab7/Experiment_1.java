package lab7;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * Simple word count sorter
 * 
 * @author drewu
 *
 */
public class Experiment_1
{

	/**
	 * The number of reducers we are using
	 */
	private final static int numOfReducers = 2;

	/**
	 * Main method where all the magic happens
	 * 
	 * @param args
	 * @throws Exception
	 */
	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception
	{
		// Spark configuration and context
		SparkConf sparkConf = new SparkConf().setAppName("WordCount in Spark");
		JavaSparkContext context = new JavaSparkContext(sparkConf);

		// Gather up all of our lines from the text file
		JavaRDD<String> lines = context.textFile("/cpre419/gutenberg");

		/**
		 * Flatten each line into word array lists
		 */
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>()
		{
			// Split through whitespace
			public Iterable<String> call(String s)
			{
				return Arrays.asList(s.split("\\s+"));
			}
		});

		/**
		 * Separate out our words and attach a count of 1 to each key
		 */
		JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>()
		{
			public Tuple2<String, Integer> call(String s)
			{
				return new Tuple2<String, Integer>(s, 1);
			}
		});

		/**
		 * Each key will be combined through the reducer
		 */
		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>()
		{
			public Integer call(Integer i1, Integer i2)
			{
				return i1 + i2;
			}
		}, numOfReducers);

		/**
		 * Just swapping the key and value around
		 */
		JavaPairRDD<Integer, String> newCounts = counts
				.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>()
				{
					public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception
					{
						return new Tuple2<Integer, String>(t._2, t._1);
					}
				}).sortByKey();

		newCounts.saveAsTextFile("lab7/tests/output/");
		context.stop();
		context.close();

	}
}