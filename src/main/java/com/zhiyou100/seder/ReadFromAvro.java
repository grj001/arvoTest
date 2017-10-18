package com.zhiyou100.seder;

import java.io.File;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import com.zhiyou100.schema.UserActionLog;

//Avro序列化
public class ReadFromAvro {

	public static void main(String[] args) throws Exception {
		File file = new File("userlogaction.avro");
		
		DatumReader<UserActionLog> reader = 
				new SpecificDatumReader<UserActionLog>();
		
		DataFileReader<UserActionLog> fileReader = 
				new DataFileReader<UserActionLog>(file, reader);
		
		UserActionLog readUserActionLog = null;
		
		while(fileReader.hasNext()){
			readUserActionLog = fileReader.next();
			System.out.println(readUserActionLog);
		}
		
		fileReader.close();
	}
	
}
