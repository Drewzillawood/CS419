package lab8;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Experiment_1
{

	private final static int numberOfReducers = 2;

	@SuppressWarnings("serial")
	public static void main(String[] args)
	{
		SparkConf sparkConf = new SparkConf().setAppName("Exp 1");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		JavaRDD<String> s = context.textFile("/cpre419/github.csv");

		/**
		 * We want to gather up all of our useful lines
		 */
		JavaPairRDD<String, Iterable<String>> lines = s.mapToPair(new PairFunction<String, String, String>()
		{

			public Tuple2<String, String> call(String s) throws Exception
			{
				String[] tokens = s.split(",");
				String language = tokens[1];
				String repo = tokens[0];
				String stars = tokens[12];
				return new Tuple2<String, String>(language, repo + " " + stars);
			}

		}).groupByKey(numberOfReducers);

		/**
		 * Count for each language
		 */
		JavaPairRDD<Integer, String> counted = lines.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, Integer, String>()
		{
			public Tuple2<Integer, String> call(Tuple2<String, Iterable<String>> t) throws Exception
			{
				String language = t._1;
				String nameOfMaxStars = "";
				int maxStars = -1;
				int count = 0;

				for (String s : t._2)
				{
					String[] tokens = s.split(" ");
					if (Integer.parseInt(tokens[1]) > maxStars)
					{
						nameOfMaxStars = tokens[0];
						maxStars = Integer.parseInt(tokens[1]);
					}
					count++;
				}
				return new Tuple2<Integer, String>(count, language + " " + nameOfMaxStars + " " + maxStars);
			}
		}).sortByKey(false);

		JavaRDD<String> sorted = counted.map(new Function<Tuple2<Integer, String>, String>()
		{
			public String call(Tuple2<Integer, String> t) throws Exception
			{
				String[] tokens = t._2.split(" ");
				String formatted = String.format("%-15s %-10d %-50s %-10s", tokens[0], t._1, tokens[1], tokens[2]);
				return formatted;
			}
		});

		sorted.saveAsTextFile("/user/drewu/lab8/exp1/output");
		context.stop();
		context.close();
	}

}
