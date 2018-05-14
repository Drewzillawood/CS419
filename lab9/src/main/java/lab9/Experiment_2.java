package lab9;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;



@SuppressWarnings("serial")
public class Experiment_2
{
	public static void main(String[] args) throws Exception
	{
		final ParameterTool params = ParameterTool.fromArgs(args);
		
		// set up execution environment 
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);
		
		// Get input data
		DataStream<String> text = env.readTextFile("hdfs:///cpre419/shakespeare");
		
		// Generate and count bigrams
		DataStream<Tuple2<String, Integer>> bigrams = 
				text
				.flatMap(new Tokenizer())
				.keyBy(0)
				.sum(1);
		
		bigrams.addSink(new Experiment_1.CustomSinkFunction());
		
		env.execute("Bigrams counter");
	}
	
	/**
	 * Separate text into bigrams
	 */
	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>>
	{
		@Override
		public void flatMap(String s, Collector<Tuple2<String, Integer>> o) throws Exception
		{
			// Tokenize our line into sentences
			String[] tokens = s.toLowerCase().split("[.!?]");
			
			// For each sentence get an array of their strings
			for(String _s : tokens)
			{
				// Emit each bigram as a Tuple with a count of 1
				String[] words = _s.split("\\W+");
				for(int i = 0; i < words.length - 1; i++)
					if(!words[i].isEmpty() && !words[i+1].isEmpty())
						o.collect(new Tuple2<String, Integer>(words[i] + " " + words[i+1], 1));
			}
		}
	}
}
