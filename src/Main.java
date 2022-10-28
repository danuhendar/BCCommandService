
public class Main {
	public static void main(String args[]) {
		try {
			IDMBCCommandService b = new IDMBCCommandService();
			b.Run();
		}catch(Exception exc) {
			exc.printStackTrace();
		}
	}
}
