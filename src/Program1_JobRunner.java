import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;


@SuppressWarnings("deprecation")
public class Program1_JobRunner extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			return -1;
		}
		//Setting Job Confs.
		Job job = new Job(getConf(), "IMDB Analytics");
		job.setJarByClass(getClass());
		
		//Reading Paths from command line arguments
		Path namesPath = new Path(args[0]);
		Path rolesPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		//Creating Distributed Cache File
		DistributedCache.addCacheFile(new URI(args[3]), job.getConfiguration());
		
		//Creating Multiple Input Mappers conf
		MultipleInputs.addInputPath(job, namesPath, TextInputFormat.class, NamesMapper.class);
		MultipleInputs.addInputPath(job, rolesPath, TextInputFormat.class, RolesMapper.class);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		//Setting Output Classes
		job.setMapOutputKeyClass(Text.class);
		job.setReducerClass(Program1_Reducer.class);
		job.setOutputKeyClass(Text.class);
		
		//Waiting for Job Completion
		return job.waitForCompletion(true) ? 0 : 1;
	}
  
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Program1_JobRunner(), args);
		System.exit(exitCode);
	}
}
