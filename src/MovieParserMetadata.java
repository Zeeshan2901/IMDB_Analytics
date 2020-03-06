import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.IOUtils;



public class MovieParserMetadata {
 
	private Map<String, String[]> movieMap = new HashMap<String, String[]>();

	public boolean ifExists(String Id){
		if (movieMap.containsKey(Id))
			return true;
		else return false;
	}
	
	public int returnSize(){
		return movieMap.size();
	}

	public void initialize(File file) throws IOException {
		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
			//MovieParser parser = new MovieParser();
			String line;
			while ((line = in.readLine()) != null) {
				String columns[]= line.split("\t");
				String str[]= {columns[1],columns[2],columns[3]};
				if (!columns[2].contentEquals("\\N"))
					movieMap.put(columns[0],str);
				
				/*if (parser.isParseable(line)) {
					parser.parse(line);
					String str[]= {parser.getGenre(), parser.getTitle(), Integer.toString(parser.getreleaseYear())};
					movieMap.put(parser.getId(),str);
				}*/
			}
		} finally {
		IOUtils.closeStream(in);
		}
	}
	
	public Map<String, String[]> getIdToNameMap() {
	    return Collections.unmodifiableMap(movieMap);
	}

	public String getTitle(String movieId) {
		String title[] = movieMap.get(movieId);
		if (title.length <3) {
			return movieId; // no match: fall back to ID
		}
		return title[0];
	}
 
	 public String getGenre(String movieId) {
		 String title[] = movieMap.get(movieId);
		 if (title.length <3) {
			 return movieId; // no match: fall back to ID
		 }
		 return title[2];
	 }
 
	 public String getReleaseYear(String movieId) {
		 String title[] = movieMap.get(movieId);
		 if (title.length <3) {
			return movieId; // no match: fall back to ID
		 }
		 return title[1];
	 }
 
	 public boolean isWestern(String movieId){
		 boolean s = getGenre(movieId).toLowerCase().contains("western") ;
		 return s;
	 }
 
}
