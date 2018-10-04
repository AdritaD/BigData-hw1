import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class MutualFriends
{
	public static class Map
	extends Mapper<LongWritable, Text, Text, Text>
	{
	
		private Text word = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] line= value.toString().split("\t");
			if (line.length ==2) 
			{
				//set friend1 - first number
				String friend1 = line[0];
				List<String> values =Arrays.asList(line[1].split( ","));
				//now iterate over list of friends and compare the values
				for (String friend2 : values)
				{
					int f1 =Integer.parseInt(friend1);
					int f2 =Integer.parseInt(friend2);
					if(f1 < f2)
						word.set(friend1 + "," + friend2);
					else
						word.set(friend2 +","+ friend1);
					context.write(word , new Text(line[1]));
				}
		    }
	    }

	}

	
	public static class Reduce extends Reducer<Text,Text,Text,Text>
	{
		private Text result =new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			HashMap<String, Integer> map = new HashMap<String,Integer>();
			StringBuilder sb= new StringBuilder();
			for (Text friends : values) 
			{
				List<String> temp = Arrays.asList(friends.toString().split(","));
				for (String friend :temp) 
				{
					if (map.containsKey(friend))
						sb.append(friend + ',');
					else
						map.put(friend,  1);
				}
			}
			
			if (sb.lastIndexOf(",")> -1) 
			{
				sb.deleteCharAt(sb.lastIndexOf(","));
			}
			
			result.set(new Text(sb.toString()));
			context.write(key,  result);
		}
	}

	

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: MutualFriends <in> <out>");
			System.exit(2);
		}

		// create a job 
		Job job = new Job(conf, "Friends");
		job.setJarByClass(MutualFriends.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);




		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}


	
	
	