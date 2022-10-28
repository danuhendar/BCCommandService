/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import com.mongodb.MongoClient;
import java.io.*;
import org.eclipse.paho.client.mqttv3.*;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Base64;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;
import java.util.Scanner;
import java.util.UUID;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import sun.misc.*;
/**
 *
 * @author Mr.Danu
 */
public class Global_function {
    Interface_ga inter_login;
    SQLConnection sqlcon = new SQLConnection();
    Connection con;
    MqttClient client_transreport = null; 
    public Global_function(){
       	Read_Setting_ini();  
       	con = sqlcon.get_connection_db(getIp_database(),getUser_database(),getPass_database(),getPort_database(),getNama_database());
           inter_login  = new Implement_ga(con);
           get_ConnectionMQtt();
           is_proses_setting_main();
           if(con == null) {
           	System.out.println("TIDAK KONEK KE DB UTAMA");
           	publish_main_setting();
           }else {
           	System.out.println("KONEK KE DB UTAMA");
           }
           
       }


    String ip_broker;
       String port_broker;
       String username_broker;
       String password_broker;
       String cleansession;
       String keepalive;
       String reconnect;
       String will_retained;
       String is_mongo_db;
       String ip_mongodb;
       String port_mongodb;
       String max_inflight;
       String ip_database;
       String user_database;
       String pass_database;
       String port_database;
       String nama_database;
       String id_reporter;
       String cabang;
       String topic;
       String tampilkan_query_console;
       
       public String getTampilkan_query_console() {
   		return tampilkan_query_console;
   	}

   	public void setTampilkan_query_console(String tampilkan_query_console) {
   		this.tampilkan_query_console = tampilkan_query_console;
   	}
   			
       
       
       	public String getCabang() {
   		return cabang;
   	}

   	public void setCabang(String cabang) {
   		this.cabang = cabang;
   	}

   	public String getTopic() {
   		return topic;
   	}

   	public void setTopic(String topic) {
   		this.topic = topic;
   	}

       	public String getIp_broker() {
   		return ip_broker;
   	}

   	public void setIp_broker(String ip_broker) {
   		this.ip_broker = ip_broker;
   	}

   	public String getPort_broker() {
   		return port_broker;
   	}

   	public void setPort_broker(String port_broker) {
   		this.port_broker = port_broker;
   	}

   	public String getUsername_broker() {
   		return username_broker;
   	}

   	public void setUsername_broker(String username_broker) {
   		this.username_broker = username_broker;
   	}

   	public String getPassword_broker() {
   		return password_broker;
   	}

   	public void setPassword_broker(String password_broker) {
   		this.password_broker = password_broker;
   	}

   	public String getCleansession() {
   		return cleansession;
   	}

   	public void setCleansession(String cleansession) {
   		this.cleansession = cleansession;
   	}

   	public String getKeepalive() {
   		return keepalive;
   	}

   	public void setKeepalive(String keepalive) {
   		this.keepalive = keepalive;
   	}

   	public String getReconnect() {
   		return reconnect;
   	}

   	public void setReconnect(String reconnect) {
   		this.reconnect = reconnect;
   	}

   	public String getWill_retained() {
   		return will_retained;
   	}

   	public void setWill_retained(String will_retained) {
   		this.will_retained = will_retained;
   	}

   	public String getIs_mongo_db() {
   		return is_mongo_db;
   	}

   	public void setIs_mongo_db(String is_mongo_db) {
   		this.is_mongo_db = is_mongo_db;
   	}

   	public String getIp_mongodb() {
   		return ip_mongodb;
   	}

   	public void setIp_mongodb(String ip_mongodb) {
   		this.ip_mongodb = ip_mongodb;
   	}

   	public String getPort_mongodb() {
   		return port_mongodb;
   	}

   	public void setPort_mongodb(String port_mongodb) {
   		this.port_mongodb = port_mongodb;
   	}

   	public String getMax_inflight() {
   		return max_inflight;
   	}

   	public void setMax_inflight(String max_inflight) {
   		this.max_inflight = max_inflight;
   	}

   	public String getIp_database() {
   		return ip_database;
   	}

   	public void setIp_database(String ip_database) {
   		this.ip_database = ip_database;
   	}

   	public String getUser_database() {
   		return user_database;
   	}

   	public void setUser_database(String user_database) {
   		this.user_database = user_database;
   	}

   	public String getPass_database() {
   		return pass_database;
   	}

   	public void setPass_database(String pass_database) {
   		this.pass_database = pass_database;
   	}

   	public String getPort_database() {
   		return port_database;
   	}

   	public void setPort_database(String port_database) {
   		this.port_database = port_database;
   	}

   	public String getNama_database() {
   		return nama_database;
   	}

   	public void setNama_database(String nama_database) {
   		this.nama_database = nama_database;
   	}

   	public String getId_reporter() {
   		return id_reporter;
   	}

   	public void setId_reporter(String id_reporter) {
   		this.id_reporter = id_reporter;
   	}


   public void Read_Setting_ini() {
       	try {
   		    JSONParser parser = new JSONParser();
   	        JSONObject obj = null;
   	        FileReader fr = new FileReader("setting.ini");
   	        BufferedReader br = new BufferedReader(fr);
   	        String line = br.readLine();
   	        System.out.println("setting ini : "+line);
   	        try {
   	            obj = (JSONObject) parser.parse(line);
   	            
   	        } catch (org.json.simple.parser.ParseException ex) {
   	            ex.printStackTrace();
   	        }
   	        
   	        setIp_broker(obj.get("ip_broker").toString());
   	        setPort_broker(obj.get("port_broker").toString());
   	        setUsername_broker(obj.get("username_broker").toString());
   	        setPassword_broker(obj.get("password_broker").toString());
   	        setCleansession(obj.get("cleansession").toString());
   	        setKeepalive(obj.get("keepalive").toString());
   	        setReconnect(obj.get("reconnect").toString());
   	        setWill_retained(obj.get("will_retained").toString());
   	        setIs_mongo_db(obj.get("is_mongo_db").toString());
   	        setIp_mongodb(obj.get("ip_mongodb").toString());
   	        setPort_mongodb(obj.get("port_mongodb").toString());
   	        setMax_inflight(obj.get("max_inflight").toString());
   	        setIp_database(obj.get("ip_database").toString());
   	        setUser_database(obj.get("user_database").toString());
   	        setPass_database(obj.get("pass_database").toString());
   	        setPort_database(obj.get("port_database").toString());
   	        setNama_database(obj.get("nama_database").toString());
   	        setId_reporter(obj.get("id_reporter").toString());
   	        setCabang(obj.get("cabang").toString());
   	        setTopic(obj.get("topic").toString());
   	        setTampilkan_query_console(obj.get("tampilkan_query_console").toString());
   	        System.out.println("Load Setting Sukses");
       	}catch(Exception exc) {
       		exc.printStackTrace();
       	}
       }
       

   public boolean is_proses_setting_main() {
       	boolean res = false;
       	try {
       		
       		//Properties p = new Properties(); 
   	        //p.load(new FileInputStream("setting.ini"));
   	        String id_reporter = getId_reporter();
   	         
       		int qos_message = 0;
       		String rtopic_command = "SETTING_MAIN/"+id_reporter+"/";
       	    System.out.println("SUBS : "+rtopic_command);
               client_transreport.subscribe(rtopic_command,qos_message,new IMqttMessageListener() {
          
                           @Override
                           public void messageArrived(final String topic, final MqttMessage message) throws Exception {
                               //----------------------------- FILTER TOPIC NOT CONTAINS -------------------------------//
                                       
                               Date HariSekarang_run = new Date();
                               String payload = new String(message.getPayload());

                               String msg_type = "";
                               String message_ADT_Decompress = "";
                               try{
                                   message_ADT_Decompress = ADTDecompress(message.getPayload());
                                   msg_type = "json";
                               }catch(Exception exc){
                                   message_ADT_Decompress = payload;
                                   msg_type = "non json";
                               }

                                
                               UnpackJSON(message_ADT_Decompress);
                               //System.out.println(message_ADT_Decompress);
                               PrintMessage2("RECV > "+rtopic_command+"",1,msg_type,topic,Parser_TASK,Parser_FROM,Parser_TO,null,HariSekarang_run);
                               File f = new File("settting.ini");
                               if(f.exists()) {
                               	f.delete();
                               }else {
                               	
                               }
                               
                               FileWriter fw = new FileWriter("setting.ini");
                               BufferedWriter bw = new BufferedWriter(fw);
                               bw.write(Parser_HASIL);
                               bw.flush();
                               bw.close();
                               fw.close();
                               System.out.println("Tulis Konfigurasi Sukses");
   			    System.exit(0);
                           }
                       });
                       
       		res = true;
       	}catch(Exception exc) {
       		res =false;
       	}
       	
       	return res;
       }

   public void PublishMessageNotDocumenter(String topic,byte[] content,int counter,String plain_text_res_message,int qos){
           try {
             
               
               SimpleDateFormat sformat = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
               Date HariSekarang = new Date();
               JSONParser parser = new JSONParser();
               //-- sesi koneksi db --//
               //String file_attribute           = ReadFile("attribute");
               //String new_attribute            = sqlcon.DecodeString(file_attribute);
               //System.out.println("new_attribute : "+new_attribute);
               //String sp_new_attribute[]       = new_attribute.split("~");
               //String broker_primary[]         = sp_new_attribute[0].split(":");
               
               String res_broker_primary       = getIp_broker()+":"+getPort_broker();
             
               String res_username_primary     = getUsername_broker();
               String res_password_primary     = getPassword_broker();
               
               /* ssl://mqtt.cumulocity.com:8883 for a secure connection */
               //-------------------------------- TRANS CONNECTION ----------------------//
               MqttClient send_transreport = null;
               
               try{
                  
                   final String serverUrl   = "tcp://"+res_broker_primary;
                   //System.out.println("serverUrl : "+serverUrl);
                   String clientId = UUID.randomUUID().toString();
                   MemoryPersistence persistence = new MemoryPersistence();
                   send_transreport = new MqttClient(serverUrl, clientId,persistence);
                   MqttConnectOptions options = new MqttConnectOptions();
                   options.setCleanSession(true);
                   options.setKeepAliveInterval(1000);
                   options.setAutomaticReconnect(true);
             
                   if(res_username_primary.equals("null")||res_password_primary.equals("null")){
                       
                   }else if(res_username_primary.equals("")||res_password_primary.equals("")) {
                   	
                   }else{
                   
                       options.setUserName(res_username_primary);
                       options.setPassword(res_password_primary.toCharArray());
                   }
                  
                   send_transreport.connect(options);
                   MqttMessage message = new MqttMessage(content);
                   message.setQos(qos);
                   //message.setRetained(res_will_retained);
                   send_transreport.publish(topic, message);
                   //System.err.println("Publish Message");
                   send_transreport.disconnect();
                   UnpackJSON(plain_text_res_message);
                   PrintMessage2("SEND > "+Parser_TASK, counter, "json", topic, Parser_TASK, Parser_FROM, Parser_TO, HariSekarang, HariSekarang);
               }catch(Exception exc){
                  exc.printStackTrace();
               }
           } catch (Exception e) {
               e.printStackTrace();
           }
       } 

   public void publish_main_setting() {
      	try {
      		
            String id_reporter = getId_reporter();
               
      		 Parser_TASK = "MAIN_SETTING";
      		 Parser_ID = get_tanggal_curdate();
      		 Parser_SOURCE = "IDMReporter";
      		 Parser_COMMAND = getTopic().toString();
      		 Parser_OTP = "";
      		 Parser_TANGGAL_JAM = "";
      		 Parser_VERSI = "";
      		 Parser_HASIL = "";
      		 Parser_FROM = id_reporter;
      		 Parser_TO = "IDMReporter";
      		 Parser_SN_HDD = "";
      		 Parser_IP_ADDRESS = "";
      		 Parser_STATION = "";
      		 Parser_CABANG = getCabang();
      		 Parser_NAMA_FILE = "";
      		 Parser_CHAT_MESSAGE = "";
      		 Parser_REMOTE_PATH = "";
      		 Parser_LOCAL_PATH = "";
      		 Parser_SUB_ID = get_tanggal_curdate_curtime();
      		 
               
           String res_message = CreateMessage(Parser_TASK,Parser_ID,Parser_SOURCE,Parser_COMMAND,Parser_OTP,Parser_TANGGAL_JAM,Parser_VERSI,Parser_HASIL,Parser_FROM,Parser_TO,Parser_SN_HDD,Parser_IP_ADDRESS,Parser_STATION,Parser_CABANG,"",Parser_NAMA_FILE,Parser_CHAT_MESSAGE,Parser_REMOTE_PATH,Parser_LOCAL_PATH,Parser_SUB_ID);
           //System.err.println("res_message_otp : "+res_message);
           byte[] convert_message = res_message.getBytes("US-ASCII");
           byte[] bytemessage = compress(convert_message);
           String topic_dest = "SETTING_MAIN/";
           System.out.println("TOPIC DEST : "+topic_dest);
           PublishMessageNotDocumenter(topic_dest, bytemessage, 1, res_message,1);
               
      	}catch(Exception exc) {
      		System.out.println("Gagal publish ke Broker atas prosedure pengambilan setting main");
      	}
      }

    
   
    long previousJvmProcessCpuTime = 0;
    long previousJvmUptime = 0;
    String write_log;
    boolean res_write_log = false;
    Global_variable gv = new Global_variable();
  
   String Parser_TASK,
    Parser_ID,
    Parser_SOURCE,
    Parser_COMMAND,
    Parser_OTP,
    Parser_TANGGAL_JAM,
    Parser_VERSI,
    Parser_HASIL,
    Parser_FROM,
    Parser_TO,
    Parser_SN_HDD,
    Parser_IP_ADDRESS,
    Parser_STATION,
    Parser_CABANG,
    Parser_NAMA_FILE,
    Parser_CHAT_MESSAGE,
    Parser_REMOTE_PATH,
    Parser_LOCAL_PATH,
    Parser_SUB_ID; 
 
    
     
    public void UnpackJSON(String json_message){
        JSONParser parser = new JSONParser();
        JSONObject obj = null;
        try {
            obj = (JSONObject) parser.parse(json_message);
        } catch (org.json.simple.parser.ParseException ex) {
            ex.printStackTrace();
        }
        
        Parser_TASK = obj.get("TASK").toString();
        //gv.setParser_TASK(obj.get("TASK").toString());
        try{
            Parser_ID = obj.get("ID").toString();
            //gv.setParser_TASK(obj.get("ID").toString());
        }catch(Exception exc){
             gv.setParser_TASK("");
        }

        Parser_SOURCE = obj.get("SOURCE").toString();
        //gv.setParser_SOURCE(obj.get("SOURCE").toString());
        Parser_COMMAND = obj.get("COMMAND").toString();
        //gv.setParser_COMMAND(obj.get("COMMAND").toString());
        Parser_OTP = obj.get("OTP").toString();
        //gv.setParser_OTP(obj.get("OTP").toString());
        try{
           Parser_TANGGAL_JAM = obj.get("TANGGAL_JAM").toString();
           //gv.setParser_TANGGAL_JAM(obj.get("TANGGAL_JAM").toString());
        }catch(Exception exc){
            Parser_TANGGAL_JAM = "";
            //gv.setParser_TANGGAL_JAM("");
        }
        try{
            Parser_VERSI = obj.get("RESULT").toString().split("_")[7];
            //gv.setParser_VERSI(obj.get("RESULT").toString().split("_")[7]);
        }catch(Exception exc){
            Parser_VERSI = obj.get("VERSI").toString();
            //gv.setParser_VERSI(obj.get("VERSI").toString());
        }


        Parser_HASIL = obj.get("HASIL").toString();
        //gv.setParser_HASIL(obj.get("HASIL").toString());
        Parser_FROM = obj.get("FROM").toString();
        //gv.setParser_FROM(obj.get("FROM").toString());
        Parser_TO = obj.get("TO").toString();
        //gv.setParser_TO(obj.get("TO").toString());
        try{
            Parser_SN_HDD = Parser_HASIL.split("_")[3];
            //String hasil = gv.getParser_HASIL().split("_")[3];
            //gv.setParser_SN_HDD(hasil);
        }catch(Exception exc){
            //gv.setParser_SN_HDD(obj.get("SN_HDD").toString());
            Parser_SN_HDD = obj.get("SN_HDD").toString();
        }
        try{
            //String hasil = gv.getParser_HASIL().split("_")[4];
            //gv.setParser_IP_ADDRESS(hasil);
            Parser_IP_ADDRESS = Parser_HASIL.split("_")[4];
        }catch(Exception exc){
            try{
                //gv.setParser_IP_ADDRESS(obj.get("IP_ADDRESS").toString());
                Parser_IP_ADDRESS = obj.get("IP_ADDRESS").toString();
            }catch(Exception exc1){
                //gv.setParser_IP_ADDRESS("");
                Parser_IP_ADDRESS = "";
            }

        }
        
        try{
            Parser_STATION = Parser_HASIL.split("_")[2];
        }catch(Exception exc){
            Parser_STATION = obj.get("STATION").toString();;
        }
        
        Parser_CABANG = obj.get("CABANG").toString();
        try{
            Parser_NAMA_FILE = obj.get("NAMA_FILE").toString();
        }catch(Exception exc){
            Parser_NAMA_FILE = "";
        }
        try{
            Parser_CHAT_MESSAGE = obj.get("CHAT_MESSAGE").toString();
        }catch(Exception exc){
            Parser_CHAT_MESSAGE = "";
        }
        try{
            Parser_REMOTE_PATH = obj.get("REMOTE_PATH").toString();
        }catch(Exception exc){
            Parser_REMOTE_PATH = "";
        }
        try{
            Parser_LOCAL_PATH = obj.get("LOCAL_PATH").toString();
        }catch(Exception exc){
            Parser_LOCAL_PATH = "";
        }
        try{
            Parser_SUB_ID = obj.get("SUB_ID").toString();
        }catch(Exception exc){
            Parser_SUB_ID = "";
        }
        
    }
    
    public void MongoLogger() {
    	try {
    		Logger mongoLogger = Logger.getLogger( "org.mongodb.driver" );
    		mongoLogger.setLevel(Level.SEVERE); 
    	}catch(Exception exc) {
    		exc.printStackTrace();
    	}
    }
    
    public Document Create_document(String KDCAB,
					            String TASK,
					            String ID,
					            String SUB_ID,
					            String SOURCE,
					            String FROM,
					            String TO,
					            String OTP,
					            String STATION,
					            String IP,
					            String IN_KDTK,
					            String IN_NAMA_PC,
					            String SN_HDD,
					            String CMD,
					            String RESULT,
					            String CHAT_MSG,
					            String NAMA_FILE,
					            String REMOTE_PATH,
					            String LOCAL_PATH,
					            String DATE_TIME,
					            String VERSION,
					            String ADDTIME){
					
					
					Document doc = new Document("KDCAB",KDCAB)
					        .append("TASK",TASK)
					        .append("ID",ID)
					        .append("SUB_ID",SUB_ID)
					        .append("SOURCE",SOURCE)
					        .append("FROM",FROM)
					        .append("TO",TO)
					        .append("OTP",OTP)
					        .append("KDTK",IN_KDTK)
					        .append("NAMA_PC",IN_NAMA_PC)
					        .append("STATION",STATION)
					        .append("IP",IP)
					        .append("SN_HDD",SN_HDD)
					        .append("CMD",CMD)
					        .append("RESULT",RESULT)
					        .append("CHAT_MSG",CHAT_MSG)
					        .append("NAMA_FILE",NAMA_FILE)
					        .append("REMOTE_PATH",REMOTE_PATH)
					        .append("LOCAL_PATH",LOCAL_PATH)
					        .append("DATE_TIME",DATE_TIME)
					        .append("VERSION",VERSION)
					        .append("rowid",new ObjectId())
					        .append("ADDTIME",ADDTIME)                 
					;
		return doc;
    }  
    
    public boolean createCollection(String table,String nama_database) {
    	boolean res = false;
    	try {
    		
    		 String ip_mongodb = "";
             String port_mongodb = "";
             //Properties p = new Properties();
             try {
                 
                 ip_mongodb = getIp_mongodb();
                 port_mongodb = getPort_mongodb();
             } catch (Exception ex) {
                  System.err.println("ERROR READING setting.ini"+ex.getMessage());
             }
             
    		com.mongodb.client.MongoClient mongo = MongoClients.create("mongodb://"+ip_mongodb+":"+port_mongodb);  //(1)
            MongoDatabase db = mongo.getDatabase(nama_database);
             
             
    		MongoIterable <String> collection =  db.listCollectionNames();
    	    for(String s : collection) {
    	        if(s.equals(table)) {
    	        	System.out.println("Collection Exists");
    	        	res = true;
    	        }else {
    	        	db.createCollection(table);
    	        	System.out.println("Collection Tidak Exists");
    	        }
    	    }
            mongo.close();
    	}catch(Exception exc) {
    		res = false;
    	}
    	return res;
    }



 public void InsertDocument(String nama_database,String table,Document doc){
        try{
            String ip_mongodb = "";
            String port_mongodb = "";
            
            try {
                
                ip_mongodb = getIp_mongodb();
                port_mongodb = getPort_mongodb();
            } catch (Exception ex) {
                 System.err.println("ERROR READING setting.ini"+ex.getMessage());
            }

            com.mongodb.client.MongoClient mongo = MongoClients.create("mongodb://"+ip_mongodb+":"+port_mongodb);  //(1)
            MongoDatabase db = mongo.getDatabase(nama_database);
            boolean cek_collection = createCollection(table,nama_database);
            db.getCollection(table).insertOne(doc);
            mongo.close();
        }catch(Exception exc){
            exc.printStackTrace();
        }
       
}

   
    public boolean create_table_mysql(String nama_table) {
	   	boolean res = false;
	   	try {
	   		String get_syntax_generate_table = GetTransReport("SELECT CONTENT FROM setting WHERE ID = '1'", 1, true);
	   		inter_login.call_upd_fetch(get_syntax_generate_table.replace("transreport", nama_table), false);
	   		res = true;
	   	}catch(Exception exc) {
	   		res = false;
	   	}
	   	
	   	return res;
  	
    }
    
    public void upd_last_sync() {
    	try {
    		 int h = Calendar.getInstance().get(Calendar.HOUR_OF_DAY);
             String jam = "";
             if(h<10){
                 jam = "0"+h;
             }else{
                 jam = ""+h;
             }
    		 
    		 WriteFile("last_sync_toko.txt", "", jam, false);
    	}catch(Exception exc) {}
    }
    
    public String readSyncDataToko() {
    	String res = "";
    	try {
    		File f = new File("last_sync_toko.txt");
    		if(f.exists()) {
    			res = ReadFile("last_sync_toko.txt");
    		}else {
    			res = "File tidak ada";
    		}
    	}catch(Exception exc) {res = exc.toString();}
    	return res;
    }
    
    public String proses_sync_data_toko(String param_kode_cabang) {
    	String res = "";
    	String data_master_toko[] = GetTransReport("SELECT KDCAB,TOKO,NAMA,IP,STATION FROM tokomain WHERE KDCAB LIKE '"+param_kode_cabang+"%' AND STATION NOT IN('','STB')", 5, false).split("~");
		JSONArray arr = new JSONArray();
		JSONObject obj = new JSONObject();
		for(int a = 0;a<data_master_toko.length;a++) {
			//String sp_field[] = data_master_program[a].split("%");)
			String sp_field[] = data_master_toko[a].split("%");
			String res_kdcab = 	sp_field[0];
			String res_kdtk = 	sp_field[1].trim();
			String res_nama = sp_field[2];
			String res_ip = sp_field[3];
			String res_station = sp_field[4];
			
			arr.add(res_kdcab);
			arr.add(res_kdtk);
			arr.add(res_nama);
			arr.add(res_ip);
			arr.add(res_station);
			obj.put(res_ip, arr.toJSONString());
			arr.clear();
		}
		
		File f = new File("master_toko_"+param_kode_cabang+".txt");
		if(f.exists()) {
			f.delete();
		}
		
		try {
			FileWriter fw = new FileWriter("master_toko_"+param_kode_cabang+".txt");
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(obj.toJSONString());
			bw.flush();
			bw.close();fw.close();
			//-- tulis log last sync data toko --//
			upd_last_sync();
		}catch(Exception exc) {}
		
		res = "Sync data toko selesai";
		return res;
		 
    }

    public String store_toko_to_local(String param_kode_cabang) {
    	String res = "";
    	try {
    		String last_sync = readSyncDataToko();
    		if(last_sync.equals("File tidak ada")) {
    			System.out.println("File tidak ada");
    			res = proses_sync_data_toko(param_kode_cabang);
    		}else {
    			int h = Calendar.getInstance().get(Calendar.HOUR_OF_DAY);
                String jam = "";
                if(h<10){
                    jam = "0"+h;
                }else{
                    jam = ""+h;
                }
                
                //-- jika last sync sama maka tidak perlu sync data toko lagi --//
                if(last_sync.equals(jam)) {
                	res = "Tidak perlu sync data toko. Jam "+last_sync+" VS "+jam+" sama.";
                //-- jika last sync beda maka perlu sync ulang data toko --//
                }else {
                	res = proses_sync_data_toko(param_kode_cabang);
                }
        		
    		}
		}catch(Exception exc) {
			res = exc.toString();
		}
    	
    	return res;
		
    } 
    
    public String get_toko_of_local_storage(String ip,int index,String branch_code) {
    	String res = "";
    	try {
    		String json_data = "";
    		if(branch_code.equals("")) {
    			ReadFile("master_toko.txt");
    		}else {
    			ReadFile("master_toko_"+branch_code+".txt");
    		}
    		
    		
    		JSONParser parser = new JSONParser();
    		JSONObject obj = null;
    		try {
    			obj = (JSONObject) parser.parse(json_data);
    		} catch (org.json.simple.parser.ParseException ex) {
    			System.out.println("message error : " + ex.getMessage());
    		}
    		
    	 
    		try {
    			String content = obj.get(ip).toString();
    			JSONArray jarray =  (JSONArray) parser.parse(content);
    			res = jarray.get(index).toString();
    		} catch (Exception ex) {
    			res = "";
    		}
    		
    	}catch(Exception exc) {exc.printStackTrace();}
    	
    	return res;
    }
    
    public String InsTransReport(String IN_TASK,
                                    String IN_ID,
                                    String IN_SOURCE,
                                    String IN_COMMAND,
                                    String IN_OTP,
                                    String IN_TANGGAL_JAM,
                                    String IN_VERSI,
                                    String IN_HASIL,
                                    String IN_FROM,
                                    String IN_TO,
                                    String IN_SN_HDD,
                                    String IN_IP_ADDRESS,
                                    String IN_STATION,
                                    String IN_CABANG,
                                    String IN_NAMA_FILE,
                                    String IN_CHAT_MESSAGE,
                                    String IN_REMOTE_PATH,
                                    String IN_LOCAL_PATH,
                                    String IN_SUB_ID,
                                    boolean command_output,
                                    String INS_OR_REPLACE,
                                    String NAMA_TABLE){
        String res = "";
        String query = "";
        try {
           
            String kdtk = "";
            String nm_pc = "";
            String res_in_from = "";
            String res_in_to = "";
            String res_in_kdcab = IN_CABANG;
            try{
                if(IN_SOURCE.equals("IDMCommandListeners")){
                    try{
                     
                    		res_in_from = IN_TO;
                    		res_in_to = IN_FROM;
                    		 
                        	String get_kdtk[] = GetTransReport("SELECT TOKO,STATION,KDCAB FROM tokomain where IP = '"+IN_IP_ADDRESS+"'", 3, false).split("~")[0].split("%");
	                        kdtk = get_kdtk[0];
	                        IN_STATION = get_kdtk[1];
	                        res_in_kdcab = get_kdtk[2];
                        	/*
                        	try {
                        		String left_kode_cabang = res_in_from.substring(0, 4);
	                 			res_in_kdcab = get_toko_of_local_storage(IN_IP_ADDRESS,0,left_kode_cabang);
	                 			kdtk = get_toko_of_local_storage(IN_IP_ADDRESS,1,left_kode_cabang);
	                 			nm_pc = get_toko_of_local_storage(IN_IP_ADDRESS,2,left_kode_cabang);
	                 			IN_STATION = get_toko_of_local_storage(IN_IP_ADDRESS,4,left_kode_cabang);
                        	}catch(Exception exc) {
                        		System.err.println("ERROR GET DATA MASTER TOKO : "+IN_IP_ADDRESS);
                        	}
                        	*/
                 			 
                            try {
                             	String get_nm_pc = GetTransReport("SELECT NAMA_PC FROM initreport where IP = '"+IN_IP_ADDRESS+"'", 1, true);
                             	nm_pc = get_nm_pc;
                            }catch(Exception exc1) {
                             	 nm_pc = "-";
                            }
                  }catch(Exception exc){
                    	exc.printStackTrace();
                        kdtk = "-";nm_pc = "-";
                  }

                }else if(IN_SOURCE.equals("IDMCommander")){
                	res_in_from = IN_TO;
                    res_in_to = IN_FROM;
                    try{
                       try {
                    	   kdtk = IN_FROM.split("_")[1].substring(0, 4);
                           nm_pc = IN_FROM.split("_")[1];
                       }catch(Exception exc) {
                    	   
                    	   //System.out.println("SELECT TOKO,STATION,KDCAB FROM tokomain where IP = '"+res_in_to+"'");
                    	   String get_kdtk[] = GetTransReport("SELECT TOKO,STATION,KDCAB FROM tokomain where IP = '"+res_in_to+"'", 3, false).split("~")[0].split("%");
                           kdtk = get_kdtk[0];
                           IN_STATION = get_kdtk[1];
                           res_in_kdcab = get_kdtk[2];
                           try {
                           	String get_nm_pc = GetTransReport("SELECT NAMA_PC FROM initreport where IP = '"+res_in_to+"'", 1, true);
                           	nm_pc = get_nm_pc;
                           	
                           }catch(Exception exc1) {
                           	 nm_pc = "-";
                           }
                           
                    	   /*
                    	   try {
                    		    String left_kode_cabang = res_in_from.substring(0, 4);
                    		    
	                 			res_in_kdcab = get_toko_of_local_storage(res_in_to,0,left_kode_cabang);
	                 			kdtk = get_toko_of_local_storage(res_in_to,1,left_kode_cabang);
	                 			nm_pc = get_toko_of_local_storage(res_in_to,2,left_kode_cabang);
	                 			IN_STATION = get_toko_of_local_storage(res_in_to,4,left_kode_cabang);
	                 			
                       	  }catch(Exception exc1) {
                       		System.err.println("ERROR GET DATA MASTER TOKO : "+res_in_to);
                       	  }
                       */
                       }
                    
                       
                     
                       
                    }catch(Exception exc){
                    	exc.printStackTrace();
                        kdtk = "-";
                        nm_pc = "-";
                    }

                }else if(IN_SOURCE.equals("IDMReporter")){
                	res_in_from = IN_FROM;
                    res_in_to = IN_TO;
                      
                    if(IN_TASK.equals("RESINITSTORE")){
                        try{
                           kdtk = IN_HASIL.split("_")[1].substring(0, 4);
                           nm_pc = IN_HASIL.split("_")[1];
                        }catch(Exception exc){
                           kdtk = IN_HASIL.substring(0, 4);
                           nm_pc = IN_HASIL;
                        }
                    }

                    else
                    {
                        kdtk = "";
                        nm_pc = "";
                    }
                }

                if(IN_IP_ADDRESS.contains("|")){
                    IN_IP_ADDRESS = "";
                }else{

                }
            }catch(Exception exc){
                kdtk = "";
                nm_pc = "";
            }
           
           
            String tahun_bulan_tanggal = get_tanggal_curdate().replaceAll("-", "");
            String nama_table_create = NAMA_TABLE+""+tahun_bulan_tanggal;
            //System.out.println("SELECT EXISTS(SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_NAME = '"+nama_table_create+"') AS CEK;");
            boolean cek_table = inter_login.cek("SELECT EXISTS(SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_NAME = '"+nama_table_create+"') AS CEK;");
            if(cek_table == false){
              	  String sql_create = "SELECT EXISTS(SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_NAME = '"+nama_table_create+"') AS CEK;";
              	  boolean create_table = create_table_mysql(nama_table_create);
              	  inter_login.call_upd_fetch(sql_create, false);
                //System.out.println("SUKSES CREATE TABLE TRANSREPORT");
            }else{
                
            }
            
            if(NAMA_TABLE == "transreport"){
                  query = INS_OR_REPLACE+" INTO "+nama_table_create+" VALUES('"+res_in_kdcab+"',"
                                                    + "'"+IN_TASK.toUpperCase()+"',"
                                                    + "'"+IN_ID+"',"
                                                    + "'"+IN_SUB_ID+"',"
                                                    + "'"+IN_SOURCE+"',"
                                                    + "'"+res_in_from+"',"
                                                    + "'"+res_in_to+"',"
                                                    + "'"+IN_OTP+"',"
                                                    + "'"+kdtk+"',"
                                                    + "'"+nm_pc+"',"
                                                    + "'"+IN_STATION+"',"
                                                    + "'"+IN_IP_ADDRESS+"',"
                                                    + "'"+IN_SN_HDD+"',"
                                                    + "CONCAT('"+IN_COMMAND.replace("\\", "/").replaceAll("'", "")+"'),"
                                                    + "CONCAT('"+IN_HASIL.replaceAll("'", "").replace("\\", "/")+"'),"
                                                    + "'"+IN_CHAT_MESSAGE.replace("", "-")+"',"
                                                    + "'"+IN_NAMA_FILE.replace("", "-")+"',"
                                                    + "'"+IN_REMOTE_PATH.replace("", "-")+"',"
                                                    + "'"+IN_LOCAL_PATH.replace("", "-")+"',"
                                                    + "NOW(),"
                                                    + "'"+IN_VERSI+"',"
                                                    + "NULL,"
                                                    + "NOW());";  
                 
            }else if(NAMA_TABLE == "initreport"){

                   query = INS_OR_REPLACE+" INTO "+NAMA_TABLE+" VALUES('"+IN_CABANG+"',"
                                                    + "'"+IN_TASK.toUpperCase()+"',"
                                                    + "'"+IN_ID+"',"
                                                    + "'"+IN_SUB_ID+"',"
                                                    + "'"+IN_SOURCE+"',"
                                                    + "'"+IN_FROM+"',"
                                                    + "'"+IN_TO+"',"
                                                    + "'"+IN_OTP+"',"
                                                    + "'"+kdtk+"',"
                                                    + "'"+nm_pc+"',"
                                                    + "'"+IN_STATION+"',"
                                                    + "'"+IN_IP_ADDRESS+"',"
                                                    + "'"+IN_SN_HDD+"',"
                                                    + "'"+IN_COMMAND.replaceAll("[^\\.A-Za-z0-9_]", " ")+"',"
                                                    + "CONCAT('"+IN_HASIL+"'),"
                                                    + "'"+IN_CHAT_MESSAGE+"',"
                                                    + "'"+IN_NAMA_FILE+"',"
                                                    + "'"+IN_REMOTE_PATH+"',"
                                                    + "'"+IN_LOCAL_PATH+"',"
                                                    + "NOW(),"
                                                    + "'"+IN_VERSI+"',"
                                                    + "NOW());";
            }
           
            if(command_output == true && IN_SOURCE.equals("IDMCommandListeners")){
                System.err.println("query_transreport : "+query);
            }else{
                
            }
            
            if(con.isClosed()){
                con = sqlcon.get_connection_db(getIp_database(),getUser_database(),getPass_database(),getPort_database(),getNama_database());
                inter_login  = new Implement_ga(con);
                
            }
            
            if(res_in_from == "") {
          	  
            }else {
            	inter_login.call_upd_fetch(query, false);
            	Boolean is_mongo_db_backup = Boolean.parseBoolean(getIs_mongo_db());
            	if(is_mongo_db_backup.equals(true)) {
            		MongoLogger();
            		Document doc = Create_document(IN_CABANG, 
    						IN_TASK.toUpperCase(), 
    						IN_ID, 
    						IN_SUB_ID, 
    						IN_SOURCE,
    						IN_FROM,
    						IN_TO,
    						IN_OTP,
    						IN_STATION,
    						IN_IP_ADDRESS,
    						kdtk,
    						nm_pc,
    						IN_SN_HDD,
    						IN_COMMAND.replaceAll("[^\\.A-Za-z0-9_]", " "),
    						IN_HASIL.replaceAll("'", ""),
    						IN_CHAT_MESSAGE,
    						IN_NAMA_FILE,
    						IN_REMOTE_PATH,
    						IN_LOCAL_PATH,
    						get_tanggal_curdate_curtime_for_log(true),
    						IN_VERSI,
    						get_tanggal_curdate_curtime_for_log(true)
    						);
            		
            		InsertDocument("idmcmd", nama_table_create, doc);
            		System.out.println("SUKSES INSERT MONGODB");
            	}else {
            		System.out.println("TIDAK INSERT MONGODB");
            	}
                res = "SUKSES INSERT TRANSREPORT";
            }
          
            
        } catch (Exception e) {
            res = e.toString();
            e.printStackTrace();
            System.exit(0); 
            //System.err.println("query_transreport : "+query);
        }
        //System.err.println("\t\t > T1_EXECUTE_QUERY_TO_"+NAMA_TABLE+" : "+res);
        return res;
    }
    
     public String get_tanggal_curdate_curtime_format(){
        String res = "";
        try {
            String get = GetTransReport("SELECT NOW() AS TANGGAL",1,true);
            res = get;
        } catch (Exception e) {
              res = "";  
        }
        
        return res;
    }
   
    public String get_curtime(){
        String res = "";
        try {
              int year = Calendar.getInstance().get(Calendar.YEAR);
              int month = Calendar.getInstance().get(Calendar.MONTH)+1;
              String bulan = "";
              if(month<10){
                  bulan = "0"+month;
              }else{
                  bulan = ""+month;
              }
              int d = Calendar.getInstance().get(Calendar.DATE);
              String tanggal = "";
              if(d<10){
                  tanggal = "0"+d;
              }else{
                  tanggal = ""+d;
              }
              int h = Calendar.getInstance().get(Calendar.HOUR_OF_DAY);
              String jam = "";
              if(h<10){
                  jam = "0"+h;
              }else{
                  jam = ""+h;
              }
              int min = Calendar.getInstance().get(Calendar.MINUTE);
              String menit = "";
              if(min<10){
                  menit = "0"+min;
              }else{
                  menit = ""+min;
              }
              int sec = Calendar.getInstance().get(Calendar.SECOND);
              String detik = "";
              if(sec<10){
                  detik = "0"+sec;
              }else{
                  detik = ""+sec;
              }
              String concat = jam+":"+menit;
              res = concat;                      
        } catch (Exception e) {
              res = "";  
        }
        
        return res;
    }
    
    
    public String CreateMessage(String IN_TASK,
                              String IN_ID,
                              String IN_SOURCE,
                              String IN_COMMAND,
                              String IN_OTP,
                              String IN_TANGGAL_JAM,
                              String IN_VERSI,
                              String IN_HASIL,
                              String IN_FROM,
                              String IN_TO,
                              String IN_SN_HDD,
                              String IN_IP_ADDRESS,
                              String IN_STATION,
                              String IN_CABANG,
                              String IN_FILE,
                              String IN_NAMA_FILE,
                              String IN_CHAT_MESSAGE,
                              String IN_REMOTE_PATH,
                              String IN_LOCAL_PATH,
                              String IN_SUB_ID           
        ){
        String res = "";
        try {
            JSONObject obj = new JSONObject();
            obj.put("TASK",IN_TASK);
            obj.put("ID",IN_ID);
            obj.put("SOURCE",IN_SOURCE);
            obj.put("COMMAND",IN_COMMAND);
            obj.put("OTP",IN_OTP);
            obj.put("TANGGAL_JAM",IN_TANGGAL_JAM);
            obj.put("VERSI",IN_VERSI);
            obj.put("HASIL",IN_HASIL);
            obj.put("FROM",IN_FROM);
            obj.put("TO",IN_TO);
            obj.put("SN_HDD",IN_SN_HDD);
            obj.put("IP_ADDRESS",IN_IP_ADDRESS);
            obj.put("STATION",IN_STATION);
            obj.put("CABANG",IN_CABANG);
            obj.put("FILE",IN_FILE);
            obj.put("NAMA_FILE",IN_NAMA_FILE);
            obj.put("CHAT_MESSAGE",IN_CHAT_MESSAGE);
            obj.put("REMOTE_PATH",IN_REMOTE_PATH);
            obj.put("LOCAL_PATH",IN_LOCAL_PATH);
            obj.put("SUB_ID",IN_SUB_ID);

            res = obj.toJSONString();
        } catch (Exception e) {
            res = e.getMessage();
        }
        
        return res;
    }
    
    
      public void PublishNotCompressAndDocumenter(String topic,String pesan){
          try {
          
            int qos = 0;
            String msg_type = "non compress";
            SimpleDateFormat sformat = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
            Date HariSekarang = new Date();
            JSONParser parser = new JSONParser();
            
            //-- sesi koneksi db --//
            String file_attribute           = ReadFile("attribute");
            String new_attribute            = sqlcon.DecodeString(file_attribute);
            //System.out.println("new_atCEKAKTItribute : "+new_attribute);
            String sp_new_attribute[]       = new_attribute.split("~");
            String broker_primary[]         = sp_new_attribute[0].split(":");
            
            String res_broker_primary       = broker_primary[0]+":"+broker_primary[1];
          
            String res_username_primary     = broker_primary[2];
            String res_password_primary     = broker_primary[3];
            
           
            //-------------------------------- TRANS CONNECTION ----------------------//
            
            MqttClient send_transreport = null;
            
          
               
                final String serverUrl   = "tcp://"+res_broker_primary;
                //System.out.println("serverUrl : "+serverUrl);
                String clientId = UUID.randomUUID().toString();
                MemoryPersistence persistence = new MemoryPersistence();
                send_transreport = new MqttClient(serverUrl, clientId,persistence);
                MqttConnectOptions options = new MqttConnectOptions();
                options.setCleanSession(true);
                options.setKeepAliveInterval(1000);
                options.setAutomaticReconnect(true);
          
                if(res_username_primary.equals("null")||res_password_primary.equals("null")){
                    
                }else{
                    options.setUserName(res_username_primary);
                    options.setPassword(res_password_primary.toCharArray());
                }
               
                send_transreport.connect(options);
                String res_message = pesan;
                byte[] convert_message = res_message.getBytes("US-ASCII");
                MqttMessage message = new MqttMessage(convert_message);
                message.setQos(qos);
                
                //message.setRetained(res_will_retained);
                send_transreport.publish(topic, message);
                //System.err.println("Connect Broker for Send : "+send_transreport.isConnected());
                //System.err.println("Publish Message");
                send_transreport.disconnect();
            
                UnpackJSON(res_message);
                InsTransReport(Parser_TASK,Parser_ID,Parser_SOURCE,Parser_COMMAND,Parser_OTP,Parser_TANGGAL_JAM,Parser_VERSI,Parser_HASIL,Parser_TO,Parser_FROM,Parser_SN_HDD,Parser_IP_ADDRESS,Parser_STATION,Parser_CABANG,Parser_NAMA_FILE,Parser_CHAT_MESSAGE,Parser_REMOTE_PATH,Parser_LOCAL_PATH,Parser_SUB_ID,false,"INSERT","transreport");
            
        } catch (Exception e) {
            e.printStackTrace();
        }
          
       
    }
      
   public boolean ChangeData(String query){
        boolean res = false;
        try{
            //con = sqlcon.get_connection_db();
            //inter_login = new Implement_ga(con);
            res = inter_login.call_upd_fetch(query, false);
            //sqlcon.disconnect_db(con);
        }catch(Exception exc){
            exc.printStackTrace();
            res = false;
        }
        
        return res;
    }
     public String GetTransReport(String query,int column,boolean return_one_columns){
        String res = null;
        try{
            //con = sqlcon.get_connection_db();
            //inter_login = new Implement_ga(con);
           
                if(return_one_columns == true){
                    String get = inter_login.call_get_procedure(query, column, false);
                    res = get.split("~")[0].split("%")[0];
                }else{
                    String get = inter_login.call_get_procedure(query, column, false);
                    res = get;
                }
            //sqlcon.disconnect_db(con);
        
        }catch(Exception exc){
            exc.printStackTrace();
            res = exc.getMessage();
        }
        
        return res;
        
    }
      
    public boolean GetExistsData(String query,boolean is_exit){
        boolean res = false;
        try{
            //con = sqlcon.get_connection_db();
            //inter_login = new Implement_ga(con);
            boolean get = inter_login.cek(query);    
            res = get;
            //sqlcon.disconnect_db(con);
        }catch(Exception exc){
            exc.printStackTrace();
            res = false;
        }
        
        return res;
    }   
      
    public void PublishMessageAndDocumenter(String topic,byte[] content,int counter,String plain_text_res_message,int qos){
        try {
          
            
            SimpleDateFormat sformat = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
            Date HariSekarang = new Date();
            JSONParser parser = new JSONParser();
            //-- sesi koneksi db --//
            String file_attribute           = ReadFile("attribute");
            String new_attribute            = sqlcon.DecodeString(file_attribute);
            //System.out.println("new_attribute : "+new_attribute);
            String sp_new_attribute[]       = new_attribute.split("~");
            String broker_primary[]         = sp_new_attribute[0].split(":");
            
            String res_broker_primary       = broker_primary[0]+":"+broker_primary[1];
          
            String res_username_primary     = broker_primary[2];
            String res_password_primary     = broker_primary[3];
            
            /* ssl://mqtt.cumulocity.com:8883 for a secure connection */
            //-------------------------------- TRANS CONNECTION ----------------------//
            MqttClient send_transreport = null;
            
            try{
               
                final String serverUrl   = "tcp://"+res_broker_primary;
                //System.out.println("serverUrl : "+serverUrl);
                String clientId = UUID.randomUUID().toString();
                MemoryPersistence persistence = new MemoryPersistence();
                send_transreport = new MqttClient(serverUrl, clientId,persistence);
                MqttConnectOptions options = new MqttConnectOptions();
                options.setCleanSession(true);
                options.setKeepAliveInterval(1000);
                options.setAutomaticReconnect(true);
          
                if(res_username_primary.equals("null")||res_password_primary.equals("null")){
                    
                }else{
                    options.setUserName(res_username_primary);
                    options.setPassword(res_password_primary.toCharArray());
                }
               
                send_transreport.connect(options);
                MqttMessage message = new MqttMessage(content);
                message.setQos(qos);
                //message.setRetained(res_will_retained);
                send_transreport.publish(topic, message);
                System.err.println("Publish Message");
                send_transreport.disconnect();
                UnpackJSON(plain_text_res_message);
                if(Parser_TASK.contains("LOGIN")){
                    InsTransReport(Parser_TASK,Parser_ID,Parser_SOURCE,sqlcon.EncodeString(Parser_COMMAND),Parser_OTP,Parser_TANGGAL_JAM,Parser_VERSI,sqlcon.EncodeString(Parser_HASIL),Parser_FROM,Parser_TO,Parser_SN_HDD,Parser_IP_ADDRESS,Parser_STATION,Parser_CABANG,Parser_NAMA_FILE,Parser_CHAT_MESSAGE,Parser_REMOTE_PATH,Parser_LOCAL_PATH,Parser_SUB_ID,false,"INSERT","transreport");
                }else{
                    InsTransReport(Parser_TASK,Parser_ID,Parser_SOURCE,Parser_COMMAND,Parser_OTP,Parser_TANGGAL_JAM,Parser_VERSI,Parser_HASIL,Parser_FROM,Parser_TO,Parser_SN_HDD,Parser_IP_ADDRESS,Parser_STATION,Parser_CABANG,Parser_NAMA_FILE,Parser_CHAT_MESSAGE,Parser_REMOTE_PATH,Parser_LOCAL_PATH,Parser_SUB_ID,false,"INSERT","transreport");
                }
                
                PrintMessage2("SEND > "+Parser_TASK, counter, "json", topic, Parser_TASK, Parser_FROM, Parser_TO, HariSekarang, HariSekarang);
            }catch(Exception exc){
               exc.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    } 
    
     public String ADTDecompress(byte[] data) {
        try {
          ByteArrayOutputStream bos = new ByteArrayOutputStream();
          ByteArrayInputStream bis = new ByteArrayInputStream(data);
          GZIPInputStream in = new GZIPInputStream(bis);
          byte[] buffer = new byte[1024];
          int len = 0;
          while ((len = in.read(buffer)) >= 0) {
            bos.write(buffer, 0, len);
          }
          in.close();
          bos.close();
          
          byte[]a = bos.toByteArray();
          String s = new String(a);
          return s;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
    }
    
    public byte[] compress(byte[] str) throws Exception {
        ByteArrayOutputStream obj=new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(obj);
        gzip.write(str);
        gzip.close();
        return obj.toByteArray();
    }
    
    
    // Method menghitung selisih dua waktu
    public String selisihDateTime(Date waktuSatu, Date waktuDua) {
        long selisihMS = Math.abs(waktuSatu.getTime() - waktuDua.getTime());
        long selisihDetik = selisihMS / 1000 % 60;
        long selisihMenit = selisihMS / (60 * 1000) % 60;
        long selisihJam = selisihMS / (60 * 60 * 1000) % 24;
        long selisihHari = selisihMS / (24 * 60 * 60 * 1000);
        String selisih = selisihHari + " hari " + selisihJam + " Jam "
                + selisihMenit + " Menit " + selisihDetik + " Detik";
        return selisih;
    }
  
    protected Date konversiStringkeDate(String tanggalDanWaktuStr,String pola, Locale lokal) {
        Date tanggalDate = null;
        SimpleDateFormat formatter;
        if (lokal == null) {
            formatter = new SimpleDateFormat(pola);
        } else {
            formatter = new SimpleDateFormat(pola, lokal);
        }
        try {
            tanggalDate = formatter.parse(tanggalDanWaktuStr);
        } catch (ParseException ex) {
            ex.printStackTrace();
        }
        return tanggalDate;
    }
    
    
    public boolean RestartApps(){
        boolean res = false;
        try {
            Runtime runtime = Runtime.getRuntime();
            runtime.exec("restartapps.bat");
            res = true;
        } catch (Exception e) {
            res = false;
        }
        
        return res;
    }
    
    public String ReadFile(String file){
        String data = null;
        try {
         
            File myObj = new File(file);
            Scanner myReader = new Scanner(myObj);
                    
                while (myReader.hasNextLine()) {
                    data = myReader.nextLine();
                }
                myReader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        
        return data;
    }
    
      public String ReadFileLog(String file){
        String res = null;
        try {
             
            
            File f = new File(file);
            if(f.exists()){
                Scanner myReader = new Scanner(f);
                while (myReader.hasNextLine()) {
                  String data = myReader.nextLine();
                  
                  res += data+"\n";
                }
                myReader.close();
            }else{
                try {
                    f.createNewFile();
                } catch (IOException ex) {
                    
                }
                res = "";
            }
          
          
        } catch (Exception e) {
           
        }
        
        return res;
    }
    
    
    public void writeLogTopic(String id,String sub_id,String filename,String message,boolean append_or_not){
        
        String folder = "history_broadcast";
        File f = new File(folder);
        if(f.exists()){
            
        }else{
            f.mkdir();
        }
        String path = "";
        try {
            path = new File(".").getCanonicalPath();
        } catch (IOException ex) {
            Logger.getLogger(Global_function.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        String folder_id_subid = path+"/"+folder+"/"+id+"_"+sub_id;
        System.out.println("folder idsub_id : "+folder_id_subid);
        File ffolder_id_subid = new File(folder_id_subid);
        if(ffolder_id_subid.exists()){
            
        }else{
            ffolder_id_subid.mkdir();
        }

        Logger logger = Logger.getLogger(path+"_"+folder+"_"+filename);

        // Create an instance of FileHandler that write log to a file called
        // app.log. Each new message will be appended at the at of the log file.
        FileHandler fileHandler = null;        
        try {
            fileHandler = new FileHandler(folder_id_subid+"/"+filename+".txt",append_or_not);
        } catch (IOException ex) {
            System.err.println(ex.getMessage());
        } 
        logger.addHandler(fileHandler);
        logger.info(message);
//        fileHandler.flush();
//        fileHandler.close();
    }
    
    public void del_history_log(){
        String tanggal = get_tanggal_curdate();
        String curtime = get_tanggal_curdate_curtime_for_log(false);
        
        
           
        String nama_file_except = "log_idmreporter_"+tanggal+".txt";
        String[] pathnames;

        // Creates a new File instance by converting the given pathname string
        // into an abstract pathname
        String dir = System.getProperty("user.dir");
        File f = new File(dir);

        // Populates the array with names of files and directories
        pathnames = f.list();

        // For each pathname in the pathnames array
        for (String pathname : pathnames) {
            // Print the names of files and directories
            if(pathname.equals(nama_file_except)){
                
            }else{
                System.out.println(pathname);
                if(pathname.contains("log_idmreport")){
                    f.delete();
                }
            }
            
        }
    }
    
    public void WriteFile(String file,String content_before,String content_after,boolean append_content){
        try {
            
            if(append_content == true){
                FileWriter fw = new FileWriter(file);
                BufferedWriter bw = new BufferedWriter(fw);
                bw.write(content_before+"");
                bw.write(content_after+"");
                bw.flush();
                bw.close();
                fw.close();
            }else{
                FileWriter fw = new FileWriter(file);
                BufferedWriter bw = new BufferedWriter(fw);
                bw.write(content_after+"");
                bw.flush();
                bw.close();
                fw.close();
            }
          
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
     
    public String get_tanggal_curdate(){
        String res = "";
        try {
              int year = Calendar.getInstance().get(Calendar.YEAR);
              int month = Calendar.getInstance().get(Calendar.MONTH)+1;
              
              String bulan = "";
              if(month<10){
                  bulan = "0"+month;
              }else{
                  bulan = ""+month;
              }
              int d = Calendar.getInstance().get(Calendar.DATE);
              String tanggal = "";
              if(d<10){
                  tanggal = "0"+d;
              }else{
                  tanggal = ""+d;
              }
              String concat = year+""+bulan+""+tanggal;
              res = concat;                      
        } catch (Exception e) {
              res = "";  
        }
        
        return res;
    }
     
     public String get_tanggal_curdate_curtime(){
        String res = "";
        try {
              int year = Calendar.getInstance().get(Calendar.YEAR);
              int month = Calendar.getInstance().get(Calendar.MONTH)+1;
              String bulan = "";
              if(month<10){
                  bulan = "0"+month;
              }else{
                  bulan = ""+month;
              }
              int d = Calendar.getInstance().get(Calendar.DATE);
              String tanggal = "";
              if(d<10){
                  tanggal = "0"+d;
              }else{
                  tanggal = ""+d;
              }
              int h = Calendar.getInstance().get(Calendar.HOUR_OF_DAY);
              String jam = "";
              if(h<10){
                  jam = "0"+h;
              }else{
                  jam = ""+h;
              }
              int min = Calendar.getInstance().get(Calendar.MINUTE);
              String menit = "";
              if(min<10){
                  menit = "0"+min;
              }else{
                  menit = ""+min;
              }
              int sec = Calendar.getInstance().get(Calendar.SECOND);
              String detik = "";
              if(sec<10){
                  detik = "0"+sec;
              }else{
                  detik = ""+sec;
              }
              String concat = year+"-"+bulan+"-"+tanggal+" "+jam+":"+menit+":"+detik;
              res = concat;                      
        } catch (Exception e) {
              res = "";  
        }
        
        return res;
    }
    
     public String get_tanggal_curdate_curtime_for_log(boolean format_log){
        String res = "";
        try {
              int year = Calendar.getInstance().get(Calendar.YEAR);
              int month = Calendar.getInstance().get(Calendar.MONTH)+1;
              String bulan = "";
              if(month<10){
                  bulan = "0"+month;
              }else{
                  bulan = ""+month;
              }
              int d = Calendar.getInstance().get(Calendar.DATE);
              String tanggal = "";
              if(d<10){
                  tanggal = "0"+d;
              }else{
                  tanggal = ""+d;
              }
              int h = Calendar.getInstance().get(Calendar.HOUR);
              String jam = "";
              if(h<10){
                  jam = "0"+h;
              }else{
                  jam = ""+h;
              }
              int min = Calendar.getInstance().get(Calendar.MINUTE);
              String menit = "";
              if(min<10){
                  menit = "0"+min;
              }else{
                  menit = ""+min;
              }
              int sec = Calendar.getInstance().get(Calendar.SECOND);
              String detik = "";
              if(sec<10){
                  detik = "0"+sec;
              }else{
                  detik = ""+sec;
              }
              
              String concat = "";
              if(format_log == true)
              {
                  concat = year+"-"+bulan+"-"+tanggal+" "+jam+":"+menit+":"+detik;
              }
              else
              {
                  concat = year+""+bulan+""+tanggal;
              }
                                
              res = concat;                      
        } catch (Exception e) {
              res = "";  
        }
        
        return res;
    }
     
    
     public void WriteLog(String content_after,boolean append_content){
       
        String read = "";
        try {
            String tanggal = get_tanggal_curdate();
            int jam = Calendar.getInstance().get(Calendar.HOUR_OF_DAY);
            
           
            String nama_file = "log_idmreporter_"+tanggal+"_"+jam+".txt";
           
                try{
                    read = ReadFileLog(nama_file);
                }catch(Exception exc){
                    read = "";
                }
                //System.out.println("read before : "+read);
                if(append_content == true){
                    if(read.equals("")){
                        WriteFile(nama_file, "======================= Starting IDMReporter =======================\n",""+jam+ " - "+content_after+"\n",append_content);
                    }else{
                        WriteFile(nama_file, read.replaceAll("null", ""),""+jam+ " - "+content_after+"\n",append_content);
                    }
                }else{
                     WriteFile(nama_file,"",content_after+"\n",append_content);
                }
               
                
        } catch (Exception e) {
            read = "";
        }
    }
     
    
    public void PrintMessage(String FROM_THREAD,int counter,String msg_type,String topic,String Parser_TASK,String Parser_FROM,String Parser_TO,Date HariSekarang,Date HariSekarang_run){
        //=========================================================//
        Global_variable gv = new Global_variable();
        SimpleDateFormat sformat = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
        System.out.println("\n");
        System.out.println("PUBLISH RECEIVE "+FROM_THREAD+" - "+counter+"");
        System.out.println("=========================================");
        //System.out.println("Msg\t:\t"+message_ADT_Decompress);
        System.out.println("MsgType\t:\t"+msg_type);
        System.out.println("Topic\t:\t" + topic);
        System.out.println("Task\t:\t" + Parser_TASK);
        System.out.println("From\t:\t" +Parser_FROM);
        System.out.println("To\t:\t" +Parser_TO);
        System.out.println("Date\t:\t"+sformat.format(HariSekarang));
        

    }
    
    private static final String ALGORITHM = "md5";
    private static final String DIGEST_STRING = "HG58YZ3CR9";
    private static final String CHARSET_UTF_8 = "utf-8";
    private static final String SECRET_KEY_ALGORITHM = "DESede";
    private static final String TRANSFORMATION_PADDING = "DESede/CBC/PKCS5Padding";

    /* Encryption Method */
    public byte[] encrypt(String message) throws Exception 
    { 
        final MessageDigest md = MessageDigest.getInstance(ALGORITHM); 
        final byte[] digestOfPassword = md.digest(DIGEST_STRING.getBytes(CHARSET_UTF_8)); 
        final byte[] keyBytes = Arrays.copyOf(digestOfPassword, 24); 
        for (int j = 0, k = 16; j < 8;) { 
                keyBytes[k++] = keyBytes[j++]; 
        } 
        System.out.println(new String(keyBytes));
        final SecretKey key = new SecretKeySpec(keyBytes, SECRET_KEY_ALGORITHM); 
        final IvParameterSpec iv = new IvParameterSpec(new byte[8]); 
        final Cipher cipher = Cipher.getInstance(TRANSFORMATION_PADDING); 
        cipher.init(Cipher.ENCRYPT_MODE, key, iv); 

        final byte[] plainTextBytes = message.getBytes(CHARSET_UTF_8);
        System.out.println(new String(plainTextBytes));
        final byte[] cipherText = cipher.doFinal(plainTextBytes); 

        //BASE64Encoder base64encoder = new BASE64Encoder();
        //return base64encoder.encode(cipherText);
        return cipherText; 
    } 



/* Decryption Method */
    public String decrypt(byte[] message) throws Exception { 
        final MessageDigest md = MessageDigest.getInstance(ALGORITHM); 
        final byte[] digestOfPassword = md.digest(DIGEST_STRING.getBytes(CHARSET_UTF_8)); 
        final byte[] keyBytes = Arrays.copyOf(digestOfPassword, 24); 
        for (int j = 0, k = 16; j < 8;) { 
                keyBytes[k++] = keyBytes[j++]; 
        } 
        System.out.println(new String(keyBytes));
        final SecretKey key = new SecretKeySpec(keyBytes, SECRET_KEY_ALGORITHM); 
        final IvParameterSpec iv = new IvParameterSpec(new byte[8]); 
        final Cipher decipher = Cipher.getInstance(TRANSFORMATION_PADDING); 
        decipher.init(Cipher.DECRYPT_MODE, key, iv); 

        final byte[] plainText = decipher.doFinal(message); 

        return new String(plainText); 
    }

    
    public void PrintMessage2(String FROM_THREAD,int counter,String msg_type,String topic,String Parser_TASK,String Parser_FROM,String Parser_TO,Date HariSekarang,Date HariSekarang_run){
        //=========================================================//
        Global_variable gv = new Global_variable();
        SimpleDateFormat sformat = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
        //System.out.println("\n");
        System.out.println(FROM_THREAD+" : "+counter+" >> "+Parser_FROM);
    }
    
    public void PrintMessage3(String FROM_THREAD,int counter,String msg_type,String topic,String Parser_TASK,String Parser_FROM,String Parser_TO,Date HariSekarang,Date HariSekarang_run){
        //=========================================================//
        Global_variable gv = new Global_variable();
        SimpleDateFormat sformat = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
        //System.out.println("\n");
        System.out.println(counter+" >> "+Parser_FROM.split("_")[0]+"_"+Parser_FROM.split("_")[1]);
    }
      
    private static SecretKeySpec secretKey;
    private static byte[] key;
 
    public static void setKey(String myKey) throws UnsupportedEncodingException 
    {
        MessageDigest sha = null;
        try {
            key = myKey.getBytes("UTF-8");
            sha = MessageDigest.getInstance("SHA-1");
            key = sha.digest(key);
            key = Arrays.copyOf(key, 16); 
            secretKey = new SecretKeySpec(key, "AES");
        } 
        catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } 
        catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }
 
    public static String encrypt(String strToEncrypt, String secret) 
    {
        try
        {
            setKey(secret);
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE, secretKey);
            return Base64.getEncoder().encodeToString(cipher.doFinal(strToEncrypt.getBytes("UTF-8")));
        } 
        catch (Exception e) 
        {
            System.out.println("Error while encrypting: " + e.toString());
        }
        return null;
    }
    
    public void ins_versi_program_toko(String IN_Parser_HASIL,String IN_IP_Address) {
    	try {
    		
    		WriteLog("HASIL PROGRAM : "+IN_Parser_HASIL, true);
    		if(IN_IP_Address.contains("192.168.")) {
    			System.out.println("Tidak mendokumentasikan ip 192.168.");
    		}else {
    			String sql_data_toko = "SELECT KDCAB,TOKO,NAMA,IP,STATION FROM tokomain WHERE IP = '"+IN_IP_Address+"'";
    			
    			String get_data_toko[] = GetTransReport(sql_data_toko, 5, false).split("~")[0].split("%");
    			String data_kode_cabang = get_data_toko[0];
    			String data_kode_toko = get_data_toko[1];
    			String data_nama = get_data_toko[2];
    			String data_kode_ip = get_data_toko[3];
    			String data_kode_station = get_data_toko[4];
    			
        		String json_hasil = "";
    			String res_hasil[] = IN_Parser_HASIL.split("\n");
    			JSONArray script_list = new JSONArray();
    			//ArrayList script_list = new ArrayList();
    			
    			for(int b = 0;b<res_hasil.length;b++) {
    						JSONObject obj_cab = new JSONObject();
    						JSONArray script_data = new JSONArray();
    						String res_hasil_sp_field[] = res_hasil[b].split(";");
    						
    						String nama = "";
    						try {
    							nama = res_hasil_sp_field[0].trim().toLowerCase();
    						}catch(Exception exc) {nama = "ERROR";}
    						//obj_cab.put("NAMA", nama);
    						if(nama.contains("!UVNCPFT-") || nama.contains("uvncpft")) {
    							WriteLog("UVNCPFT : "+nama, true);
    						
    						}else {
    							
    							String versi_program_master = "";
    							String size_program_master = "";
    							String nama_program_master = "";
    							try {
    								String data_master_program = GetTransReport("SELECT VERSI,SIZE,NAMA_PROGRAM FROM m_table_setting_program WHERE NAMA_PROGRAM = '"+nama+"';", 3, false).split("~")[0];
    								versi_program_master = 	data_master_program.split("%")[0];
    								size_program_master = 	data_master_program.split("%")[1].trim();
    								nama_program_master = data_master_program.split("%")[2].trim().toLowerCase();
    							}catch(Exception exc) {versi_program_master = "ERROR";size_program_master = "ERROR";nama_program_master = "ERROR";}
    							
    							//-- jika nama program toko tidak sama dengan nama program master maka tidak perlu disimpan --//
    							if(nama.equals(nama_program_master)) {
    								script_data.add(nama);
    								
    								String versi = "";
    								try {
    									if(res_hasil_sp_field[1].equals("")) {
    										versi = "0";
    									}else {
    										versi = res_hasil_sp_field[1].trim();
    										//System.out.println("versi program toko : "+versi);
    										//System.out.println("versi program toko replace : "+versi.toString().replace(".", ""));
    									}
    								}catch(Exception exc) { versi = "0";}
    								//obj_cab.put("VERSI", versi);
    								script_data.add(versi);
    								
    								
    								//-- proses pengecekan status NOK VERSI DAN NOK SIZE --//
    								String size = "";
    								String status = "";
    								try {
    									size = res_hasil_sp_field[3].trim();
    									if(size.equals(size_program_master) && versi.equals(versi_program_master)) {
    										status = "OK";
    									}else {
    										
    										try {
    											//System.out.println("VERSI TOKO VS VERSI SERVER : "+Integer.parseInt(versi.toString().replace(".", "")) +" - "+Integer.parseInt(versi_program_master.toString().replace(".", "")));
    											
    											if(Integer.parseInt(versi.toString().replace(".", "")) > Integer.parseInt(versi_program_master.toString().replace(".", ""))) {
    												status = "NOK Program Melebihi Versi Server|";
    											}else if(versi != versi_program_master){
    												status = "NOK VERSI|";
    											}else {
    												status = "";
    											}
    											
    											if(Integer.parseInt(size) != Integer.parseInt(size_program_master)) {
    												if(status.equals("")) {
    													status = "NOK SIZE";
    												}else {
    													status = status+"NOK SIZE";
    												}
    												
													//System.out.println("SIZE TOKO VS SIZE SERVER : "+size +" - "+size_program_master);
												}else{
													 
												}
    										}catch(Exception exc) {
    											status = "NOK VERSI EXC";
    										}
    										
    										
    									}
    									
    								}catch(Exception exc) { exc.printStackTrace();  status = exc.toString(); }
    								
    								String last_write = "";
    								try {
    									last_write = res_hasil_sp_field[2].trim();
    								}catch(Exception exc) {last_write = "ERROR";}
    								//obj_cab.put("LAST_WRITE", last_write);
    								
    								 
    								 
    								script_data.add(size);
    								script_data.add(status);
    								script_data.add(last_write);
    								script_data.add(get_tanggal_curdate_curtime());
    								script_list.add(script_data);
    								json_hasil = script_list.toJSONString();
    								
    								
    							//-- jika nama program toko sama dengan nama program master maka perlu disimpan --//	
    							}else {
    								WriteLog(nama+":"+nama.length()+" VS "+nama_program_master+":"+nama_program_master.length(), true);
    							}
    							//-- end of pengecekan nama program vs nama program toko --//
    							//-- nama,versi_program_master,versi,size_program_master,size,last_write,status
    							
    						}		
    			}
    			
    			String sql_transaksi = "REPLACE INTO transaksi_versi_program_toko VALUES('"+data_kode_cabang+"','"+data_kode_toko+"','"+data_kode_station+"','"+json_hasil+"',NOW());";
				//String sql_transaksi = "REPLACE INTO transaksi_versi_program_toko VALUES('"+data_kode_cabang+"','"+data_kode_toko+"','"+data_kode_station+"','"+data_kode_ip+"','"+nama+"','"+versi+"','"+versi_program_master+"','"+size+"','"+size_program_master+"','"+last_write+"','"+status+"',NOW(),'ServiceVersiProgram');";
				//System.err.println(sql_transaksi);
				//WriteLog("sql_transaksi : "+sql_transaksi, true);
				ChangeData(sql_transaksi);
    		}
    	}catch(Exception exc) {exc.printStackTrace();}
    }
 
    public static String decrypt(String strToDecrypt, String secret) 
    {
        try
        {
            setKey(secret);
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING");
            cipher.init(Cipher.DECRYPT_MODE, secretKey);
            return new String(cipher.doFinal(Base64.getDecoder().decode(strToDecrypt)));
        } 
        catch (Exception e) 
        {
            System.out.println("Error while decrypting: " + e.toString());
        }
        return null;
    }
    
    
    public MqttClient get_ConnectionMQtt() {
    	//Properties p = new Properties();
    	int res_keepalive = 60;
    	Boolean res_cleansession = false;   
    	int qos_message = 1;
	        
	        try {
	            //p.load(new FileInputStream("setting.ini"));
	            //String maxvmusepercent = p.getProperty("maxvmusepercent");
	            String  cleansession = getCleansession(); //p.getProperty("cleansession");
	            res_cleansession = Boolean.parseBoolean(cleansession);
	            System.out.println("res_cleansession : "+res_cleansession);
	            
	            String keepalive = getKeepalive();//p.getProperty("keepalive");
	            res_keepalive = Integer.parseInt(keepalive);
	            
	            String reconnect = getReconnect();//p.getProperty("reconnect");
	            Boolean res_reconnect = Boolean.parseBoolean(reconnect);
	            System.out.println("res_reconnect : "+res_reconnect);
	            
	            String will_retained =  getWill_retained();//p.getProperty("will_retained");
	            Boolean res_will_retained = Boolean.parseBoolean(will_retained);
	            System.out.println("will_retained\t:\t"+res_will_retained);
	            
	            String ip_mongo_db = getIp_mongodb();//p.getProperty("ip_mongodb");
	            System.out.println("ip_mongo_db : "+ip_mongo_db);
	            
	        } catch (Exception ex) {
	            ex.printStackTrace();
	        }
	        
	     
	           
	         
	            SimpleDateFormat sformat = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
	            Date HariSekarang = new Date();
	            JSONParser parser = new JSONParser();
	            //-- sesi koneksi db --//
	            String file_attribute           = ReadFile("attribute");
	            String new_attribute            = sqlcon.DecodeString(file_attribute);
	            //System.out.println("new_attribute : "+new_attribute);
	            String sp_new_attribute[]       = new_attribute.split("~");
	            //String broker_primary[]         = sp_new_attribute[0].split(":");
	            
	            String res_broker_primary       = getIp_broker()+":"+getPort_broker();//broker_primary[0]+":"+broker_primary[1];
	          
	            String res_username_primary     = "";
	            if(getUsername_broker().equals("")){
	            	//broker_primary[2];
	            	res_username_primary = "";
	            }else {
	            	res_username_primary = getUsername_broker();
	            }
	            String res_password_primary     = "";
	            if(getPassword_broker().equals("")){
	            	//broker_primary[2];
	            	res_password_primary = "";
	            }else {
	            	res_password_primary = getPassword_broker();
	            }
	            //getPassword_broker();//broker_primary[3];

	            /* ssl://mqtt.cumulocity.com:8883 for a secure connection */
	            //-------------------------------- TRANS CONNECTION ----------------------//
	            try{
	                final String serverUrl   = "tcp://"+res_broker_primary;
	                
	                String clientId = UUID.randomUUID().toString();
	                MemoryPersistence persistence = new MemoryPersistence();
	                client_transreport = new MqttClient(serverUrl, clientId,persistence);
	                MqttConnectOptions options = new MqttConnectOptions();
	                options.setCleanSession(res_cleansession);
	             
			options.setKeepAliveInterval(res_keepalive);
	                options.setAutomaticReconnect(true);
	                
	                if(res_username_primary.equals("null")||res_password_primary.equals("null")){
	                    
	                }else if(res_username_primary.equals("")||res_password_primary.equals("")) {
	                
	                }else{
	                
	                    options.setUserName(res_username_primary);
	                    options.setPassword(res_password_primary.toCharArray());
	                }
	               
	                client_transreport.connect(options);
	                //client_transreport_login.connect(options);
	                System.out.println("Konek ke Broker : "+res_broker_primary);
	            }catch(Exception exc){
	               exc.printStackTrace();
	            }
	            	
    	return client_transreport;
    	
    }
    
    public String EncodeString(String plain_text){
        String encodedString = "";
        try{
            String originalInput = plain_text;
            encodedString = Base64.getEncoder().encodeToString(originalInput.getBytes());
        }catch(Exception exc){
            exc.printStackTrace();
        }
        return encodedString;   
    } 
    
    public void get_MonitoringResources(){
        try {
            
             /*
                Mencari selisih tanggal dari curdate dan tanggal up
            */
            SimpleDateFormat sformat = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
            Locale lokal = null;
            String pola = "dd-MM-yyyy HH:mm:ss";

            
            DecimalFormat format = new DecimalFormat("##.##");
            long total, free, used;
            int mb = 1024*1024;
            Runtime runtime = Runtime.getRuntime();
            total = runtime.totalMemory();
            free = runtime.freeMemory();
            used = total - free;
            double used_percent_memory = (double)used/(double)total*100;
            double free_percent_memory = (double)free/(double)total*100;
            
            //System.out.println("VMMem\t:\t" + total / mb + " MB\t"+"Used\t:\t" + used / mb + " MB "+format.format(used_percent_memory)+ " %"+"\tFree\t:\t" + free / mb + " MB "+format.format(free_percent_memory)+ " %");
           
            int batas_memory = 50;    
            if(used_percent_memory > batas_memory){
                System.out.println("Exit IDMReporter Memory Usage > "+batas_memory+" % : Memory Saat ini : "+used_percent_memory+" %");
                try {
                	Runtime.getRuntime().exec("systemctl restart BCCommandMessageService");
                }catch(Exception exc) {
                	System.exit(0);
                }
                
            }else{
                
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public void insProgramInstalled(String IN_Parser_HASIL,String IN_IP_Address) {
    	try {
    		
    		//WriteLog("HASIL PROGRAM : "+IN_Parser_HASIL, true);
    		if(IN_IP_Address.contains("192.168.")) {
    			System.out.println("Tidak mendokumentasikan ip 192.168.");
    		}else {
    			String sql_data_toko = "SELECT KDCAB,TOKO,NAMA,IP,STATION FROM tokomain WHERE IP = '"+IN_IP_Address+"';";
    			String get_data_toko[] = GetTransReport(sql_data_toko, 5, false).split("~")[0].split("%");
    			/*
    			String data_kode_cabang = get_toko_of_local_storage(IN_IP_Address,0);
    			String data_kode_toko = get_toko_of_local_storage(IN_IP_Address,1);
    			String data_nama = get_toko_of_local_storage(IN_IP_Address,2);
    			String data_kode_ip = get_toko_of_local_storage(IN_IP_Address,3);
    			String data_kode_station = get_toko_of_local_storage(IN_IP_Address,4);
    			System.out.println(get_tanggal_curdate_curtime()+" - Selesai mendapatkan data toko");
    			*/
    			String data_kode_cabang = get_data_toko[0];
    			String data_kode_toko = get_data_toko[1];
    			String data_nama = get_data_toko[2];
    			String data_kode_ip = get_data_toko[3];
    			String data_kode_station = get_data_toko[4];
    			
        		String json_hasil = "";
    			String res_hasil[] = IN_Parser_HASIL.split("\n");
    			JSONArray script_list = new JSONArray();
    			//ArrayList script_list = new ArrayList();
    			
    			for(int b = 0;b<res_hasil.length;b++) {
    						//JSONObject script_data = new JSONObject();
    						JSONArray script_data = new JSONArray();
    						if(res_hasil[b].equals(",,,")) {
    							System.err.println("1.ERROR NAMA PROGRAM : "+res_hasil[b]);
    						}else {
    							String res_hasil_sp_field[] = res_hasil[b].split(",");
        						
        						String nama = "";
        						String versi = "";
        						String publisher = "";
        						String installDate = "";
        						try {
        							nama = res_hasil_sp_field[0].replaceAll("\"", "").trim();
        						}catch(Exception exc) {nama = "-";}
        						
        						try {
        							versi = res_hasil_sp_field[1].replaceAll("\"", "").trim();
        						}catch(Exception exc) {versi = "-";}
        						
        						if(nama.equals("") || nama.contains("DisplayName")) {
        							
        						}else if(nama.contains("ERROR")){
        							System.err.println("2.ERROR NAMA PROGRAM : "+res_hasil[b]);
        						}else {
        								script_data.add(nama);
        								script_data.add(versi);
        								script_list.add(script_data);
        								json_hasil = script_list.toJSONString();
        								//System.out.println(get_tanggal_curdate_curtime()+" - Selesai proses record program "+b+": "+nama);
        						}	
    						}
    							
    			}

    			if(data_kode_cabang.equals("")) {
    				WriteLog("Data ip address tidak ditemukan : "+IN_IP_Address, true);
    			}else {
    				String sql_transaksi = "REPLACE INTO transaksi_program_installed VALUES('"+data_kode_cabang+"','"+data_kode_toko+"','"+data_kode_station+"','"+json_hasil.replace("\\\\", "\\")+"',NOW());";
    				System.err.println("SQL INSERT DATA PROGRAM : "+sql_transaksi);
        			ChangeData(sql_transaksi);
        			System.out.println(get_tanggal_curdate_curtime()+" - Selesai insert data");
    			}
    			
    		}
    	}catch(Exception exc) {exc.printStackTrace();}
    }
    
}
