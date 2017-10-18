package com.zhiyou100.seder.nogen;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;


public class AvroWriter {

	private Schema schema;
	
	
	//parser是专门用来把字符串或者avsc转换成schema对象的工具类
	private Schema.Parser parser = new Schema.Parser();
	
	
	//初始化schema, 根据序列化数据而定
	public AvroWriter(String schemaFile) throws Exception {
		
		this.schema = parser.parse(new File(schemaFile));
		
	}
	
	
	public void writeData(GenericRecord record) throws IOException{
		
		DatumWriter<GenericRecord> writer = 
				new GenericDatumWriter<GenericRecord>();
		DataFileWriter<GenericRecord> fileWriter = 
				new DataFileWriter<GenericRecord>(writer);
		
		
		fileWriter.create(
				schema, 
				new File("noobjuserlogaction.avro")
				);
		
		fileWriter.append(record);
		fileWriter.flush();
		fileWriter.close();
	}
	
	
	public static void main(String[] args) throws Exception {
		
		//用模式文件文件位置作为初始化writer序列化类
		AvroWriter avroWriter = 
				new AvroWriter("src/main/avro/user_action_log.avsc");
		
		//创建Record对象
		GenericRecord record = 
				new GenericData.Record(avroWriter.schema);
		
		record.put("userName", "jim");
		record.put("actionType", "new_tweet");
		record.put("ipAddress", "192.168.2.1");
		record.put("gender", 0);
		record.put("provience", "yunnan");
		
		avroWriter.writeData(record);
	}

	
}
