import java.io.FileInputStream;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

import org.bson.Document;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class IDMBCCommandService {
	MqttClient client_transreport;
	Global_function gf = new Global_function(true);
	Interface_ga inter_login;
	Connection con;
	SQLConnection sqlcon = new SQLConnection();
	int counter = 1;

	public IDMBCCommandService() {

	}

	String Parser_TASK, Parser_ID, Parser_SOURCE, Parser_COMMAND, Parser_OTP, Parser_TANGGAL_JAM, Parser_VERSI,
			Parser_HASIL, Parser_FROM, Parser_TO, Parser_SN_HDD, Parser_IP_ADDRESS, Parser_STATION, Parser_CABANG,
			Parser_NAMA_FILE, Parser_CHAT_MESSAGE, Parser_REMOTE_PATH, Parser_LOCAL_PATH, Parser_SUB_ID;

	public void UnpackJSON(String json_message) {

		JSONParser parser = new JSONParser();
		JSONObject obj = null;
		try {
			obj = (JSONObject) parser.parse(json_message);
		} catch (org.json.simple.parser.ParseException ex) {
			System.out.println("message json : " + json_message);
			System.out.println("message error : " + ex.getMessage());
			// ex.printStackTrace();
			// Logger.getLogger(IDMReport.class.getName()).log(Level.SEVERE, null, ex);
		}

		try {
			Parser_TASK = obj.get("TASK").toString();
		} catch (Exception ex) {
			Parser_TASK = "";
		}
		try {
			Parser_ID = obj.get("ID").toString();
		} catch (Exception exc) {
			Parser_ID = "";
		}
		try {
			Parser_SOURCE = obj.get("SOURCE").toString();
		} catch (Exception exc) {
			Parser_SOURCE = "";
		}
		try {
			Parser_COMMAND = obj.get("COMMAND").toString();
		} catch (Exception exc) {
			Parser_COMMAND = "";
		}
		try {
			Parser_OTP = obj.get("OTP").toString();
		} catch (Exception exc) {
			Parser_OTP = "";
		}

		try {
			Parser_TANGGAL_JAM = obj.get("TANGGAL_JAM").toString();
		} catch (Exception exc) {
			Parser_TANGGAL_JAM = "";
		}
		try {
			Parser_VERSI = obj.get("RESULT").toString().split("_")[7];
		} catch (Exception exc) {
			try {
				Parser_VERSI = obj.get("VERSI").toString();
			} catch (Exception exc1) {
				Parser_VERSI = "";
			}

		}

		try {
			Parser_HASIL = obj.get("HASIL").toString();
			Parser_FROM = obj.get("FROM").toString();
			Parser_TO = obj.get("TO").toString();

		} catch (Exception exc) {
			Parser_HASIL = "";
			Parser_FROM = "";
			Parser_TO = "";
		}

		try {
			Parser_SN_HDD = obj.get("SN_HDD").toString();
		} catch (Exception exc) {
			try {
				Parser_SN_HDD = obj.get("SN_HDD").toString();
			} catch (Exception exc1) {
				Parser_SN_HDD = "";
			}

		}
		try {
			Parser_IP_ADDRESS = obj.get("IP_ADDRESS").toString();
		} catch (Exception exc) {
			try {
				Parser_IP_ADDRESS = obj.get("IP_ADDRESS").toString();
			} catch (Exception exc1) {
				Parser_IP_ADDRESS = "";
			}

		}

		try {
			Parser_STATION = obj.get("STATION").toString();
		} catch (Exception exc) {
			try {
				Parser_STATION = obj.get("STATION").toString();
			} catch (Exception exc1) {
				Parser_STATION = "";
			}

		}

		try {
			Parser_CABANG = obj.get("CABANG").toString();
		} catch (Exception exc) {
			try {
				Parser_CABANG = obj.get("CABANG").toString();
			} catch (Exception exc1) {
				Parser_CABANG = "";
			}
		}

		try {
			Parser_NAMA_FILE = obj.get("NAMA_FILE").toString();
		} catch (Exception exc) {
			Parser_NAMA_FILE = "";
		}
		try {
			Parser_CHAT_MESSAGE = obj.get("CHAT_MESSAGE").toString();
		} catch (Exception exc) {
			Parser_CHAT_MESSAGE = "";
		}
		try {
			Parser_REMOTE_PATH = obj.get("REMOTE_PATH").toString();
		} catch (Exception exc) {
			Parser_REMOTE_PATH = "";
		}
		try {
			Parser_LOCAL_PATH = obj.get("LOCAL_PATH").toString();
		} catch (Exception exc) {
			Parser_LOCAL_PATH = "";
		}
		try {
			Parser_SUB_ID = obj.get("SUB_ID").toString();
		} catch (Exception exc) {
			Parser_SUB_ID = "";
		}

	}
	
	public void BCCommandService(String rtopic_command,int qos_message_command) {
		
		try {
			String branch_code = gf.en.getCabang();
			String rtopic_bc_dc =  gf.en.getTopic();
			/*
			System.out.println("Mulai Ambil data toko : "+gf.get_tanggal_curdate_curtime());			
			String hasil_sync_data_toko = gf.store_toko_to_local(branch_code);
			System.out.println("Hasil Sync : "+hasil_sync_data_toko);
			System.out.println("Selesai Ambil data toko : "+gf.get_tanggal_curdate_curtime());
			*/
			client_transreport.subscribe(rtopic_command, qos_message_command, new IMqttMessageListener() {
				@Override
				public void messageArrived(final String topic, final MqttMessage message) throws Exception {
					// ----------------------------- FILTER TOPIC NOT CONTAINS
					// -------------------------------//
					
					if (topic.contains("BYLINE")) {
						String payload = new String(message.getPayload());
						System.err.println("BYLINE > " + payload);
					} else {
						Date HariSekarang_run = new Date();
						String payload = new String(message.getPayload());

						String msg_type = "";
						String message_ADT_Decompress = "";
						try {
							message_ADT_Decompress = gf.ADTDecompress(message.getPayload());
							msg_type = "json";
						} catch (Exception exc) {
							message_ADT_Decompress = payload;
							msg_type = "non json";
						}

						counter++;
						UnpackJSON(message_ADT_Decompress);
					    //System.out.println("FROM TOPIC : "+topic+"\n");
					    
						gf.PrintMessage2("RECV > "+rtopic_command+"", counter, msg_type, topic, Parser_TASK, Parser_FROM,Parser_TO, null, HariSekarang_run);
						
						if(Parser_TO.equals("ServiceProgram") && Parser_SOURCE.equals("IDMCommandListeners")){
							String ip = Parser_IP_ADDRESS;
							gf.ins_versi_program_toko(Parser_HASIL,ip);
						}else if(Parser_TO.equals("ServiceProgramInstalled") && Parser_SOURCE.equals("IDMCommandListeners")){
							//System.out.println("SIZE MESSAGE : "+message_ADT_Decompress.length());
							//System.err.println(message_ADT_Decompress);
							String ip = Parser_IP_ADDRESS;
							gf.insProgramInstalled(Parser_HASIL,ip);
						}else if(Parser_TO.equals("ServicePhysicalDisk") && Parser_SOURCE.equals("IDMCommandListeners")){
							//System.out.println("SIZE MESSAGE : "+message_ADT_Decompress.length());
							//System.err.println(message_ADT_Decompress);
							String ip = Parser_IP_ADDRESS;
							gf.insPhysicalDisk(Parser_HASIL,ip,Parser_VERSI);
						}else {
							gf.InsTransReport(Parser_TASK, Parser_ID, Parser_SOURCE, Parser_COMMAND, Parser_OTP,
									Parser_TANGGAL_JAM, Parser_VERSI, Parser_HASIL, Parser_TO, Parser_FROM, Parser_SN_HDD,
									Parser_IP_ADDRESS, Parser_STATION, Parser_CABANG, Parser_NAMA_FILE, Parser_CHAT_MESSAGE,
									Parser_REMOTE_PATH, Parser_LOCAL_PATH, Parser_SUB_ID, Boolean.parseBoolean(gf.en.getTampilkan_query_console()), "INSERT", "transreport");
							
							String tanggal_jam = gf.get_tanggal_curdate_curtime();
							gf.WriteFile("timemessage.txt", "", tanggal_jam, false);
							
						}
								
					}
				}
			});
		} catch (MqttException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	

	public void Run() {
		System.out.println("=================================          START         ==================================");
		try {
			client_transreport = gf.get_ConnectionMQtt();
			// ---------------------------- COMMAND -----------------------//
			int qos_message_command = 0;
			String rtopic_command = gf.en.getTopic();
			System.out.println("SUBS : "+rtopic_command);
			BCCommandService(rtopic_command,qos_message_command);
		} catch (Exception exc) {
			exc.printStackTrace();
		}
	}
}
