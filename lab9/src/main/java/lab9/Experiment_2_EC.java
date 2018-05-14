package lab9;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class Experiment_2_EC
{

	public static void main(String[] args) throws Exception
	{
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		// get input data
		DataStream<String> text = env.readTextFile("hdfs:///cpre419/shakespeare");

		DataStream<Tuple2<String, Integer>> lines = 
				text
				.countWindowAll(10)
				.apply(new MyWindowFunction());

		lines.writeAsText("hdfs:///user/drewu/lab9/exp2_ec/output");

		env.execute("Extra credit WordCount");
	}

	/**
	 * Window function handles each window
	 * 
	 * @author drewu
	 *
	 */
	public static final class MyWindowFunction implements AllWindowFunction<String, Tuple2<String, Integer>, GlobalWindow>
	{	
		@Override
		public void apply(GlobalWindow w, Iterable<String> i, Collector<Tuple2<String, Integer>> c) throws Exception
		{	
			// Hash map for contstant time access to each tuple
			HashMap<String, Integer> h = new HashMap<>();
			
			// For each input line
			for (String s : i)
			{
				// Split into sentences
				String[] t = s.toLowerCase().split("[.!?]");
				
				// For each sentence
				for(String _s : t)
				{
					// Split into words
					String[] tokens = _s.split("\\W+");
					for(int j = 0; j < tokens.length - 1; j++)
						if(!tokens[j].isEmpty() && !tokens[j+1].isEmpty())
						{
							// Create bigrams
							String combined = tokens[j] + " " + tokens[j+1];
							if(h.containsKey(combined))
								h.put(combined, h.get(combined) + 1);
							else
								h.put(combined, 1);
						}
				}
			}

			PriorityQueue<Tuple2<String, Integer>> p = new PriorityQueue<>(new Experiment_1.Tuple2IntegerComparator());
			
			// For every entry in our Hash Map add to our Max Heap
			for(Map.Entry<String, Integer> m : h.entrySet())
			{
				p.add(new Tuple2<>(m.getKey(), m.getValue()));
			}
			
			// Poll the first 10 tuples (top ten counts for this window)
			for(int j = 0; j < 10; j++)
				c.collect(p.poll());
			c.collect(new Tuple2<>("__________________",null));
		}
	}
}
