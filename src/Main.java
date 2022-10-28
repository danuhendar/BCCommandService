
public class Main {
	public static void main(String args[]) {
		try {
			//IDMBCCommandService b = new IDMBCCommandService();
			//b.Run();
			
			Global_function gf = new Global_function(false);
			ThreadMain t1 = new ThreadMain(1);
			t1.start();
			
			
			String tanggal_jam = gf.get_tanggal_curdate_curtime();
			gf.WriteFile("timemessage.txt", "", tanggal_jam, false);
			
			CheckThread t2 = new CheckThread();
			t2.start();

			
		}catch(Exception exc) {
			exc.printStackTrace();
		}
	}
}
