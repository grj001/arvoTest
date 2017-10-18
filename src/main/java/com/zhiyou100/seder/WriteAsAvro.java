package com.zhiyou100.seder;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import com.zhiyou100.schema.UserActionLog;


//Avro反序列化
public class WriteAsAvro {

	public static void main(String[] args) throws IOException {
		
		//创建对象1
		UserActionLog ual1 = new UserActionLog();
		ual1.setActionType("login");
		ual1.setGender(1);
		ual1.setIpAddress("192.168.1.1");
		ual1.setProvience("河南");
		ual1.setUserName("lily");
		
		
		//创建对象2
		UserActionLog ual2 = 
				UserActionLog
				.newBuilder()
				.setActionType("logout")
				.setGender(0).setIpAddress("189.1.2.3")
				.setProvience("河北")
				.setUserName("jim")
				.build();
		
		//把两条数据写入文件中(序列化)
		DatumWriter<UserActionLog> writer = 
				new SpecificDatumWriter<>();
		
		DataFileWriter<UserActionLog> fileWriter = 
				new DataFileWriter<>(writer);
		
		//创建序列化文件
		fileWriter.create(
				UserActionLog.getClassSchema(), 
				new File("userlogaction.avro")
			);
		
		//写入内容
		fileWriter.append(ual1);
		fileWriter.append(ual2);
		
		fileWriter.flush();
		fileWriter.close();
		
	}
	
}
