import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RolesMapper extends  Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException   {
		//Converting Value to String and then String array.
		String [] columns = value.toString().split("\t");
		
		//Adding tag "role" to value so that each mapper has distinguishable values.
		String role = "role" + "\t" + columns[0] + "\t" + columns[3];
		//Fetching first two columns
		context.write(new Text(columns[2]), new Text(role));
		
		// Output Format
		//[nm1588970 , "role tt0000001 self"]
		//[nm0005690 , "role tt0000001 director"]
	
	}
}



