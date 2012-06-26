import org.codehaus.jackson.map.ObjectMapper;


public class Test {

	public static void main(String[] args) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		mapper.writeValue(System.out, "\"aaa\"");
	}

}
