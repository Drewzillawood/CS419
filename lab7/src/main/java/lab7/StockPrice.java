package lab7;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class StockPrice {

	private final static int numOfReducers = 2;

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: StockPrice <input> <output>");
			System.exit(1);
		}

		SparkConf sparkConf = new SparkConf().setAppName("Stock Price");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = context.textFile(args[0]);
		JavaPairRDD<String,String> prices = lines.mapToPair(new PairFunction<String, String, String>() {
	
			public Tuple2<String, String> call(String s) throws Exception {
				String[] tokens = s.split(",");
				String ticker = tokens[0];
				String date = tokens[1];
				String price = tokens[2];
				String year = date.substring(0, 4);
				
				String key = ticker+","+year;
				String value = date+","+price;
				return new Tuple2<String, String>(key, value);
			}
		}).cache();
		
		JavaPairRDD<String, String> stat = prices.groupByKey(numOfReducers).mapValues(new Function<Iterable<String>,String>() {
			public String call(Iterable<String> values) {
				int count = 0;
				double sum = 0;
				double sumOfSquare = 0;
				
				for (String str : values) {
					count++;
					double price = Double.parseDouble(str.split(",")[1]);
					sum += price;
					sumOfSquare += price*price;
				}
				
				double avg = sum/count;
				double var = sumOfSquare/count - avg*avg;
				double std = Math.sqrt(var);
				return count+","+avg+","+std;
			}
		});
		
		JavaPairRDD<String, Tuple2<String, String>> joinedPrice = prices
				.join(stat, numOfReducers);

		 JavaPairRDD<String, String> labeledPrice = joinedPrice.mapValues(new Function<Tuple2<String, String>, String>() {

			public String call(Tuple2<String, String> tuple) throws Exception {
				double price = Double.parseDouble(tuple._1().split(",")[1]);
				
				String[] stats = tuple._2().split(",");
				double avg = Double.parseDouble(stats[1]);
				double std = Double.parseDouble(stats[2]);
				
				if (price<avg-3*std) {
					return tuple._1+",low";
				}
				else if (price>avg+3*std) {
					return tuple._1+",high";
				}
				
				return tuple._1+",flat";
			}
		});
		
		JavaPairRDD<String, String> filteredPrice = labeledPrice.filter(new Function<Tuple2<String, String>, Boolean>() {
	
			public Boolean call(Tuple2<String, String> tuple) throws Exception {
				String value = tuple._2;
				String label = value.split(",")[2];
				if (label.equals("flat")) {
					return false;
				}
				return true;
			}
		});

		filteredPrice.saveAsTextFile(args[1]);
		context.stop();
		context.close();
	}
}