public class MovieParser {

	private String id;
	private String title;
	private int releaseYear;
	private String Genre;

	public MovieParser parse (String line){
		String [] columns = line.toString().split("\t");
		setId(columns[0]);
		setTitle(columns[1]);
		setreleaseYear(Integer.parseInt(columns[2]));
		setGenre(columns[3]);
		return this;
	}


	public boolean isParseable(String line){
		//Converting Value to String and then String array.
		String row = line.toString();
		String [] columns = row.split("\t");
		
		if (columns.length <4)
			return false;
		
		return true;
	}

	public void setId(String id){
		this.id =id;
	}
	
	public void setTitle(String t){
		this.title =t;
	}
	
	public void setreleaseYear (int x){
		this.releaseYear=x;
	}
	
	public void setGenre(String a){
		this.Genre= a;
	}
	
	
	public String getId(){
		return this.id;
	}
	
	public String getTitle(){
		return title;
	}
	
	public int getreleaseYear(){
		return releaseYear;
	}
	
	public String getGenre(){
		return Genre;
	}
}


