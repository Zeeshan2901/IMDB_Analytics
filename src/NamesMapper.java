import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NamesMapper extends  Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException   {
		//Converting Value to String array.
		String [] columns = value.toString().split("\t");
		
		//Adding tag "name" to value so that each mapper has distinguishable values.
		String name = "name" + "\t" + columns[1];
		context.write(new Text(columns[0]), new Text(name));
		
		//Output Format
		//[nm9993709 , "name Lu Bevins"]
		//[nm9993710 , "name Nestor Rudnytskyy"]
	}
}



