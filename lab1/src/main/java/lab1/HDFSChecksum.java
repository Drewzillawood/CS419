package lab1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSChecksum
{

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		String path_name = "/cpre419/bigdata";
		Path path = new Path(path_name);
		// Do calculation here
		
		FSDataInputStream in = fs.open(path);
		byte[] buffer = new byte[1000];
		in.read(1000000000, buffer, 0, 1000);
		
		byte checksum = buffer[0];
		for(int i = 1; i < 1000; i++)
			checksum ^= buffer[i];
		
		System.out.println(checksum);
		
		in.close();
		fs.close();
	}

}
