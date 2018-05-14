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
public class Experiment_1_EC 
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

		lines.writeAsText("hdfs:///user/drewu/lab9/exp1_ec/output");

		env.execute("Extra credit WordCount");
	}

	/**
	 * Very similar to experiment two except with Single words
	 * 
	 * @author drewu
	 *
	 */
	public static final class MyWindowFunction implements AllWindowFunction<String, Tuple2<String, Integer>, GlobalWindow>
	{	
		@Override
		public void apply(GlobalWindow w, Iterable<String> i, Collector<Tuple2<String, Integer>> c) throws Exception
		{	
			// Hash Map to allow constant time access to elements
			HashMap<String, Integer> h = new HashMap<>();
			
			for (String s : i)
			{
				// Split input into words
				String[] tokens = s.split("\\W+");
				// Add into our hash map and increment count if it already exists
				for (String _s : tokens)
					if (!_s.isEmpty())
						if(h.containsKey(_s))
							h.put(_s, h.get(_s) + 1);
						else
							h.put(_s, 1);
			}

			// Self sorting Max Heap
			PriorityQueue<Tuple2<String, Integer>> p = new PriorityQueue<>(new Experiment_1.Tuple2IntegerComparator());
			
			for(Map.Entry<String, Integer> m : h.entrySet())
			{
				p.add(new Tuple2<>(m.getKey(), m.getValue()));
			}
			
			// Pop the top ten elements for each window
			for(int j = 0; j < 10; j++)
				c.collect(p.poll());
			c.collect(new Tuple2<>("__________________",null));
		}
	}
}
