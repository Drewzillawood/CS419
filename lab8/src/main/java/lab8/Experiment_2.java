package lab8;

import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Experiment_2
{
	
	private final static int numOfReducers = 10;

	@SuppressWarnings({ "serial", "unused" })
	public static void main(String[] args)
	{
		/*
		 * Configuration setup 
		 */
		SparkConf sparkConf = new SparkConf().setAppName("Exp 2");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		
		// Retrieve all of our lines 
		JavaRDD<String> lines = context.textFile("/cpre419/patents.txt");

		/**
		 * Get our Tuples
		 */
		JavaPairRDD<String, Iterable<String>> tuples = lines
				.flatMapToPair(new PairFlatMapFunction<String, String, String>()
				{
					public Iterable<Tuple2<String, String>> call(String s) throws Exception
					{
						/*
						 * Here I am splitting each line by tabs, then emitting both the original
						 * and a reversal of the edges to get all possible adjacencies
						 */
						String[] tokens = s.split("\t");
						ArrayList<Tuple2<String, String>> tuples = new ArrayList<Tuple2<String, String>>();
						if (!tokens[0].equals(tokens[1]))
						{
							tuples.add(new Tuple2<String, String>(tokens[0], tokens[1]));
							tuples.add(new Tuple2<String, String>(tokens[1], tokens[0]));
						}
						return tuples;
					}

				}).groupByKey(numOfReducers);

		JavaPairRDD<String, Iterable<String>> tuplesPartitioned = tuples.repartition(numOfReducers);
		
		/**
		 * Generate all possible tuples with their respective adjacency lists
		 */
		JavaPairRDD<Tuple2<String, String>, Iterable<Iterable<String>>> grouping = tuplesPartitioned.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String, Iterable<String>>, Tuple2<String, String>, Iterable<String>>()
				{

					public Iterable<Tuple2<Tuple2<String, String>, Iterable<String>>> call(
							Tuple2<String, Iterable<String>> s) throws Exception
					{
						/*
						 * Here I am going through each key,value tuple and adding the list of values which have identical keys,
						 * at the end of the method you can see I group everything by key
						 */
						ArrayList<Tuple2<Tuple2<String, String>, Iterable<String>>> arr = new ArrayList<Tuple2<Tuple2<String, String>, Iterable<String>>>();
						for (String t : s._2)
						{
							arr.add(new Tuple2<Tuple2<String, String>, Iterable<String>>(
									new Tuple2<String, String>(s._1, t), s._2));
							arr.add(new Tuple2<Tuple2<String, String>, Iterable<String>>(
									new Tuple2<String, String>(t, s._1), s._2));
						}
						return arr;
					}

				}).groupByKey(numOfReducers);

		JavaPairRDD<Tuple2<String, String>, Iterable<Iterable<String>>> partitionedGrouping = grouping.repartition(numOfReducers);
		
		/**
		 * Generate our triangles
		 */
		JavaPairRDD<Tuple2<String, String>, Iterable<String>> triangles = partitionedGrouping.mapToPair(
				new PairFunction<Tuple2<Tuple2<String, String>, Iterable<Iterable<String>>>, Tuple2<String, String>, Iterable<String>>()
				{

					public Tuple2<Tuple2<String, String>, Iterable<String>> call(
							Tuple2<Tuple2<String, String>, Iterable<Iterable<String>>> t) throws Exception
					{
						/*
						 * To actually calculate the triangles we will use my helper method to find the intersection
						 * of all value lists attached to this key
						 */
						ArrayList<String> arr = new ArrayList<String>();
						for (Iterable<String> s_1 : t._2)
						{
							ArrayList<String> arrTemp = new ArrayList<String>();
							for (String s_2 : s_1)
								arrTemp.add(s_2);
							if (!arr.isEmpty())
								arr = intersection(arr, arrTemp);
							else
								arr.addAll(arrTemp);
						}
						return new Tuple2<Tuple2<String, String>, Iterable<String>>(t._1, arr);
					}
				});

		/**
		 * Count up all of our values
		 */
		JavaPairRDD<String, Integer> count = triangles
				.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Iterable<String>>, String, Integer>()
				{

					/*
					 * Now that we have all of our triangles and don't have to worry about duplicates, we
					 * can just count all of our values 
					 */
					public Tuple2<String, Integer> call(Tuple2<Tuple2<String, String>, Iterable<String>> t)
							throws Exception
					{
						int count = 0;
						for (String s : t._2)
							count++;
						return new Tuple2<String, Integer>("Count", count);
					}

				}).reduceByKey(new Function2<Integer, Integer, Integer>()
				{

					public Integer call(Integer v1, Integer v2) throws Exception
					{
						return v1 + v2;
					}

				}, numOfReducers);

		/**
		 * In order to take care of duplicates and reversals we must divide by 6
		 * (2 for reversals doubling our edges, and multiplied 3 for duplicates
		 * of each edge)
		 */
		JavaPairRDD<String, Integer> betterCount = count
				.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>()
				{

					public Tuple2<String, Integer> call(Tuple2<String, Integer> t) throws Exception
					{
						return new Tuple2<String, Integer>(t._1, t._2 / 6);
					}

				});

		betterCount.saveAsTextFile("/user/drewu/lab8/exp2/output");
		context.stop();
		context.close();
	}

	/**
	 * Helper method to run an intersection on two lists
	 * 
	 * @param t_1
	 * @param t_2
	 * @return
	 */
	private static <T> ArrayList<T> intersection(ArrayList<T> t_1, ArrayList<T> t_2)
	{
		ArrayList<T> arr = new ArrayList<T>();
		for (T t : t_1)
			if (t_2.contains(t))
				arr.add(t);
		return arr;
	}

}
