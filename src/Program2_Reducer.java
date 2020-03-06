import java.util.List;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Program2_Reducer extends Reducer<Text, Text, Text, Text>{

	static String movies_file_path = "movies.tsv";
	private MovieParserMetadata metadata;
	
	protected void setup(Context context) throws IOException, InterruptedException {
		try {
			metadata = new MovieParserMetadata();
			metadata.initialize(new File(movies_file_path));
		}
		catch (Exception e) {
			e.printStackTrace();
			System.err.println("###### Exception in reducer setup: " + e.getMessage());
		}
	}
	
	
	//output of RolesMapper
	//[nm1588970 , "role tt0000001 self"]
	//[nm0005690 , "role tt0000001 director"]
	//output of NamesMapper
	//[nm9993709 , "name Lu Bevins"]
	//[nm9993710 , "name Nestor Rudnytskyy"]
	
	//output after ShuffleSort phase
	//{ nm00001 - [(name SRK),(role tt00001 Actor),(role tt00001 Producer) ]}
	
	
	
	public void reduce(Text key, Iterable<Text> values,  Context context)  throws IOException, InterruptedException {
	
		String name="";
		List<String> movieId = new ArrayList<String>();
		List<String> category = new ArrayList<String>();
		
		//Finding the name and creating a list of movie ids and roles the person has played.
		for (Text value : values){
			String parts[]= value.toString().split("\t");
			if (parts[0].equals("name"))
				name=parts[1];
			if (parts[0].equals("role")){
				movieId.add(parts[1]);
				category.add(parts[2]);
			}
		}
		
		// Iterating through all the movies and filtering for year >2010 and genre = western
		for (int i=0 ; i< movieId.size() && i < category.size(); i++ ){
			String id = movieId.get(i);
			String categ = category.get(i);
			if (metadata.ifExists(id))
				for (int j =i+1; j< movieId.size() && j < category.size(); j++){
					if (id.contentEquals(movieId.get(j))){
						if ( (categ.toLowerCase().contains("director") && category.get(j).toLowerCase().contains("actor")) ||
								(categ.toLowerCase().contains("actor") && category.get(j).toLowerCase().contains("director")) ||
								(categ.toLowerCase().contains("actress") && category.get(j).toLowerCase().contains("director")) || 
								(categ.toLowerCase().contains("director") && category.get(j).toLowerCase().contains("actress")))
							context.write( new Text(name), new Text (metadata.getTitle(id) ));
					}
				}		
		}
	}
}
