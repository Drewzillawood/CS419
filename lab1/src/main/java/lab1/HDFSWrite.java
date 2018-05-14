package lab1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSWrite
{
	public static void main(String[] args) throws Exception
	{
		// The system configuration
		Configuration conf = new Configuration();
		// Get an instance of the Filesystem
		FileSystem fs = FileSystem.get(conf);
		String path_name = "/user/drewu/lab1/afile.txt";
		Path path = new Path(path_name);
		// The Output Data Stream to write into
		FSDataOutputStream file = fs.create(path);
		// Write some data
		file.writeChars("Sometihng blatantly different from everything else!");
		// Close the file and the file system instance
		file.close();
		fs.close();
	}
}