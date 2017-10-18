package com.zhiyou100.seder.nogen;

import java.io.File;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

public class AvroReader {

	public static void main(String[] args) throws Exception {

		DatumReader<GenericRecord> reader = 
				new GenericDatumReader<>();
		
		DataFileReader<GenericRecord> fileReader = 
				new DataFileReader<>(
						new File("noobjuserlogaction.avro")
						, reader);
		
		GenericRecord record = null;
		while(fileReader.hasNext()){
			record = fileReader.next();
			System.out.println(record);
		}
		
		fileReader.close();
	}

}
