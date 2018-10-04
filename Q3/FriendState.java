/* 	Import Files */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FriendState extends Configured implements Tool {
	
	
	public static class Mapper1
	extends Mapper<LongWritable, Text, Text, Text>
	{
	
		private Text word = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] line= value.toString().split("\t");
			if (line.length ==2) 
			{
				Configuration config = context.getConfiguration();

				String user1 = config.get("userA");
			    String user2 = config.get("userB");
			    int userA= Integer.valueOf(user1);
			    int userB= Integer.valueOf(user2);
			    
				//set friend1 - first number
				String friend1 = line[0];
				List<String> values =Arrays.asList(line[1].split( ","));
				//now iterate over list of friends and compare the values
				for (String friend2 : values)
				{
					int f1 =Integer.parseInt(friend1);
					int f2 =Integer.parseInt(friend2);
					if((f1 < f2 )&&((f1==userA && f2==userB)||(f1==userB && f2==userA)))
						word.set(friend1 + "," + friend2);
					else
						word.set(friend2 +","+ friend1);
					context.write(word , new Text(line[1]));
				}
		    }
	    }

	}

	
	public static class Reducer1 extends Reducer<Text,Text,Text,Text>
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

	static String test = "";
	static HashMap<String, String> storeMap;

	static HashSet<Integer> temporaryMap = new HashSet<>();

	public static class FriendData extends Mapper<Text, Text, Text, Text> {

		String friendData;

		public void setup(Context context) throws IOException {
			Configuration config = context.getConfiguration();

			storeMap = new HashMap<String, String>();
			String mydataPath = config.get("data");

			// Location of file in HDFS
			Path path = new Path("hdfs://cshadoop1" + mydataPath);
			FileSystem fs = FileSystem.get(config);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line;
			line = br.readLine();
			while (line != null) {
				String[] myArr = line.split(",");
				if (myArr.length == 10) {
					String data = myArr[1] + ":" + myArr[5];
					storeMap.put(myArr[0].trim(), data);
				}
				line = br.readLine();
			}

		}

		// int count=0;

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] split = line.split(",");

			if (null != storeMap && !storeMap.isEmpty()) {
				for (String s : split) {
					if (storeMap.containsKey(s)) {
						friendData = storeMap.get(s);
						storeMap.remove(s);
						context.write(key, new Text(friendData));
					}
				}
			}
		}
	}

	public static class Mapper2 extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			Text counter = new Text();
			String s = "";
			
			for (Text t : values) {
				if (s.equals(""))
					s = "[";
				s = s + t + ",";
			}
			
			s = s.substring(0, s.length() - 1);
			s = s + "]";
			counter.set(s);
			context.write(key, counter);
		}
	}

	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(new Configuration(), new FriendState(), args);
		System.exit(res);

	}

	public int run(String[] otherArgs) throws Exception {
		Configuration conf = new Configuration();
		if (otherArgs.length != 6) {
			System.err.println("Usage: Error");
			System.exit(2);
		}

		conf.set("userA", otherArgs[0]);
		conf.set("userB", otherArgs[1]);

		Job job = new Job(conf, "MutualFriend");
		job.setJarByClass(FriendState.class);
		
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
		Path p = new Path(otherArgs[3]);
		FileOutputFormat.setOutputPath(job, p);

		int code = job.waitForCompletion(true) ? 0 : 1;

		Configuration conf1 = getConf();
		conf1.set("data", otherArgs[4]);
		Job job2 = new Job(conf1, "sort");
		job2.setJarByClass(FriendState.class);
		job2.setInputFormatClass(KeyValueTextInputFormat.class);

		job2.setMapperClass(FriendData.class);
		job2.setReducerClass(Mapper2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, p);
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[5]));

		// Execute job and grab exit code
		code = job2.waitForCompletion(true) ? 0 : 1;

		FileSystem.get(conf).delete(p, true);
		System.exit(code);
		return code;
	}
}