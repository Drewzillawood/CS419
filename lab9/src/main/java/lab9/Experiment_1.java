package lab9;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class Experiment_1
{
	public static void main(String[] args) throws Exception
	{
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		// get input data
		DataStream<String> text = env.readTextFile("hdfs:///cpre419/gutenberg");

		DataStream<Tuple2<String, Integer>> counts =
				// split up the lines in pairs (2-tuples) containing: (word,1)
				text.flatMap(new Tokenizer())
						// group by the tuple field "0" and sum up tuple field "1"
						.keyBy(0)
						.sum(1);

		// emit result
		counts.addSink(new CustomSinkFunction());

		env.execute("Streaming WordCount Example");
	}
	
	/**
	 * Attempt at a sink class implementation
	 * 
	 * @author drewu
	 *
	 */
	public static final class CustomSinkFunction extends RichSinkFunction<Tuple2<String, Integer>>
	{
		/**
		 * Max heap
		 */
		PriorityQueue<Tuple2<String, Integer>> p;
		
		/**
		 * Initialization
		 */
		@Override
		public void open(Configuration parameters) throws Exception
		{
			// Initializing Max Heap
			super.open(parameters);
			p = new PriorityQueue<>(new Tuple2IntegerComparator());
		}
		
		/**
		 * Add to our heap
		 */
		@Override
		public void invoke(Tuple2<String, Integer> value) throws Exception
		{
			p.add(value);
		}
		
		/**
		 * Poll the top ten elements on close
		 */
		@Override
		public void close() throws IOException
		{
			
			ArrayList<Tuple2<String, Integer>> topTen = new ArrayList<>();
			while(!p.isEmpty() && topTen.size() < 10)
			{ 
				// Add first element we see
				if(topTen.isEmpty())
					topTen.add(p.poll());
				else 
				{
					// Checker for if we contain the next element on our max heap already
					// this would indicate it is less than the element we already have and
					// there for do not want it
					boolean containsKey = false;
					for(Tuple2<String, Integer> s : topTen)
						if(s.f0.equals(p.peek().f0))
						{
							containsKey = true;
							p.poll();
						}
					if(!containsKey)
						topTen.add(p.poll());
				}			
			}
			
			// Print our top ten to the log
			System.out.println("____________________________________");
			topTen.forEach((a) -> System.out.println(a));
			System.out.println("____________________________________");
		}
	}

	/**
	 * This comparator is being utilized by a priority queue which will maintain
	 * its max value at its head
	 * 
	 * @author drewu
	 *
	 * @param <T>
	 */
	public static class Tuple2IntegerComparator implements Comparator<Tuple2<?, Integer>>
	{
		@Override
		public int compare(Tuple2<?, Integer> _0, Tuple2<?, Integer> _1)
		{
			return _1.f1 - _0.f1;
		}
	}

	/**
	 * Example Tokenizer given to retrieve word counts
	 * 
	 * @author drewu
	 *
	 */
	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>>
	{
		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out)
		{
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens)
			{
				if (token.length() > 0)
				{
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}
}