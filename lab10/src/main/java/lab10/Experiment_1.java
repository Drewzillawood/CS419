package lab10;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class Experiment_1
{

	public static void main(String[] args) throws Exception
	{
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);
		
		DataStream<String> text = env.readTextFile("hdfs:///cpre419/github.csv");

		DataStream<Tuple2<String, String>> rows =
				text
				.countWindowAll(200000)
				.apply(new MyWindowFunction());
		
		rows.writeAsText("hdfs:///user/drewu/lab10/exp1/output");
		
		env.execute();
	}
	
	/**
	 * Window function handles each window
	 * 
	 * @author drewu
	 *
	 */
	@SuppressWarnings("serial")
	public static final class MyWindowFunction implements AllWindowFunction<String, Tuple2<String, String>, GlobalWindow>
	{	
		@Override
		public void apply(GlobalWindow w, Iterable<String> i, Collector<Tuple2<String, String>> o) throws Exception
		{
			// hashmap allows constant time access to each tuple
			HashMap<String, Tuple3<String, Integer, Integer>> h = new HashMap<>();
			
			/* We will organize our tuples as follows
			 * String  : Repository (Replaced when highest star is replaced)
			 * Integer : Count of repositories
			 * Integer : Highest Star
			 */
			for(String s : i)
			{
				String[] t = s.split(",");
				
				int numStars = Integer.parseInt(t[12]);
				
				if(!h.containsKey(t[1]))
					h.put(t[1], new Tuple3<>(t[0], 1, numStars));
				else
				{
					int increment = h.get(t[1]).f1 + 1;
					if(numStars > h.get(t[1]).f2)
						h.put(t[1], new Tuple3<>(t[0], increment, numStars));
					else
						h.put(t[1], new Tuple3<>(h.get(t[1]).f0, increment, h.get(t[1]).f2));
				}	
			}
			
			PriorityQueue<Tuple2<String, Tuple3<String, Integer, Integer>>> p = new PriorityQueue<>(new Tuple3IntegerComparator());
			
			for(Entry<String, Tuple3<String, Integer, Integer>> m : h.entrySet())
			{
				p.add(new Tuple2<>(m.getKey(), new Tuple3<>(m.getValue().f0, m.getValue().f1, m.getValue().f2)));
			}
			
			for(int j = 0; j < 7; j++)
			{
				Tuple2<String, Tuple3<String, Integer, Integer>> t = p.poll();
				o.collect(new Tuple2<>(t.f0, t.f1.toString()));
			}
			o.collect(new Tuple2<>("____________","_____________"));
		}
	}
	
	/**
	 * Custom comparator to sort my priority queue
	 */
	public static class Tuple3IntegerComparator implements Comparator<Tuple2<String, Tuple3<String, Integer, Integer>>>
	{

		@Override
		public int compare(Tuple2<String, Tuple3<String, Integer, Integer>> _0, Tuple2<String, Tuple3<String, Integer, Integer>> _1)
		{
			return _1.f1.f1 - _0.f1.f1;
		}

	}
}
