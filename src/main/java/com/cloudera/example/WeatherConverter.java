package com.cloudera.example;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.example.generated.WeatherRecord;

public class WeatherConverter extends Configured implements Tool {

	  public int run(String[] args) throws Exception {
		    if (args.length != 2) {
		      System.err.println("Usage: Driver <input path> <output path>");
		      return -1;
		    }

		    Job conf = new Job();
		    conf.setJobName("WeatherRecord Converter");

		    AvroJob.setOutputKeySchema(conf, WeatherRecord.SCHEMA$);

		    conf.setMapperClass(WeatherMapper.class);
		    conf.setNumReduceTasks(0);

		    conf.setInputFormatClass(TextInputFormat.class);
		    conf.setOutputFormatClass(AvroKeyOutputFormat.class);
		    
		    conf.setOutputKeyClass(AvroKey.class);
		    conf.setOutputValueClass(NullWritable.class);
		    
		    FileInputFormat.setInputPaths(conf, new Path(args[0]));
		    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		    conf.setJarByClass(WeatherConverter.class);
		    
		    conf.submit();
		    conf.waitForCompletion(true);
		    
		    return 0;
		  }

		  public static void main(String[] args) throws Exception {
		    int res = ToolRunner.run(new Configuration(), new WeatherConverter(), args);
		    System.exit(res);
		  }

}
