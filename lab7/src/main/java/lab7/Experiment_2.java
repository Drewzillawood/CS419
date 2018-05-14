package lab7;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Experiment_2
{
	private final static int numOfReducers = 2;

	@SuppressWarnings({ "serial" })
	public static void main(String[] args)
	{
		SparkConf sparkConf = new SparkConf().setAppName("Exp 2");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		JavaRDD<String> ip_lines = context.textFile("/cpre419/ip_trace");
		JavaRDD<String> raw_lines = context.textFile("/cpre419/raw_block");

		/**
		 * Gather up all of our ip_lines useful information
		 */
		JavaPairRDD<String, String> ips = ip_lines.mapToPair(new PairFunction<String, String, String>()
		{
			public Tuple2<String, String> call(String s) throws Exception
			{
				String[] tokens = s.split(" ");
				String time = tokens[0];
				String connectionID = tokens[1];
				String sourceIP = tokens[2];
				String destinationIP = tokens[4];
				return new Tuple2<String, String>(connectionID, time + " " + sourceIP + " " + destinationIP);
			}
		});

		/**
		 * Gather up all of our raw_lines useful information
		 */
		JavaPairRDD<String, String> raw = raw_lines.mapToPair(new PairFunction<String, String, String>()
		{
			public Tuple2<String, String> call(String s) throws Exception
			{
				String[] tokens = s.split(" ");
				String connectionId = tokens[0];
				String actionTaken = tokens[1];
				return new Tuple2<String, String>(connectionId, actionTaken);
			}
		}).filter(new Function<Tuple2<String, String>, Boolean>()
		{
			public Boolean call(Tuple2<String, String> s) throws Exception
			{
				return s._2.equals("Blocked");
			}
		});

		/**
		 * Join the separate tables
		 */
		JavaPairRDD<String, Tuple2<String, String>> joined = ips.join(raw, numOfReducers);

		/**
		 * Regenerating the firewall file
		 */
		JavaRDD<String> firewall = joined.map(new Function<Tuple2<String, Tuple2<String, String>>, String>()
		{
			public String call(Tuple2<String, Tuple2<String, String>> s) throws Exception
			{
				String key = s._1;
				String[] values_1 = s._2.toString().split(" ");
				String[] values_2 = values_1[2].split(",");
				return values_1[0] + " " + key + " " + values_1[1] + " " + values_2[0] + " " + values_2[1];
			}
		});

		/**
		 * Group all of our values by their sourceIPs
		 */
		JavaPairRDD<String, Iterable<String>> groupedBySourceIp = firewall.mapToPair(new PairFunction<String, String, String>()
		{
			public Tuple2<String, String> call(String s) throws Exception
			{
				String[] vals = s.split(" ");
				String key = vals[2];
				String values = vals[0] + " " + vals[1] + " " + vals[3] + " " + vals[4];
				return new Tuple2<String, String>(key, values);
			}
		}).groupByKey(numOfReducers);
		
		/**
		 * Doing the counting
		 */
		JavaPairRDD<Integer, String> blockCounts = groupedBySourceIp.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, Integer, String>()
		{
			@SuppressWarnings("unused")
			public Tuple2<Integer, String> call(Tuple2<String, Iterable<String>> s) throws Exception
			{
				Integer count = 0;
				for(String s1 : s._2)
					count++;
				return new Tuple2<Integer, String>(count, s._1);
			}

		});
		
		JavaPairRDD<Integer, String> sortedBlocks = blockCounts.sortByKey(false);

		sortedBlocks.saveAsTextFile("/user/drewu/lab7/exp2_b/output");
		context.stop();
		context.close();
	}

}
