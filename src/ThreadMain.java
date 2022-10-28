
public class ThreadMain extends Thread {
	IDMBCCommandService idm;
     
    public ThreadMain(int num){
    	idm = new IDMBCCommandService();
    }
    
    public void run(){
        for(int l = 0;l<1;l++){
           try{
        	   idm.Run();
           }catch(Exception exc){
               
           }
           
        }
    } 
}
