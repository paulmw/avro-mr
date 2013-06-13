package com.cloudera.example;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.cloudera.example.generated.WeatherRecord;

public class WeatherMapper extends Mapper<LongWritable, Text, AvroKey<WeatherRecord>, NullWritable> {

	private WeatherRecord weatherRecord = new WeatherRecord();
	private AvroKey<WeatherRecord> avroWrapper = new AvroKey<WeatherRecord>(weatherRecord);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String [] parts = value.toString().split(",");

		weatherRecord.setYear(Integer.parseInt(parts[0]));
		weatherRecord.setTemperature(Integer.parseInt(parts[1]));
		weatherRecord.setStationId(parts[2]);

		avroWrapper.datum(weatherRecord);

		context.write(avroWrapper, NullWritable.get());
	}

}
