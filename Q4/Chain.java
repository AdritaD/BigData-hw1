
 import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.hash.Hash;

public class Chain {
	
	public static class Mapper1 extends Mapper<LongWritable, Text, LongWritable, Text>
	{
		LongWritable userId=new LongWritable();
		
		public void map(LongWritable key, Text value,Context context ) throws IOException, InterruptedException 
		{
			String line[]=value.toString().split("\t");
			userId.set(Long.parseLong(line[0]));
			if(line.length==2)
			{
				String friends=line[1];
				String outvalue="U:"+friends.toString();
				context.write(userId, new Text(outvalue));
			}
		}
	}
	
	public static class Mapper2 extends Mapper<LongWritable, Text, LongWritable, Text>
	{
		LongWritable outkey=new LongWritable();
		Text outvalue=new Text();
		public void map(LongWritable key, Text value,Context context ) throws IOException, InterruptedException
		{
			String input[]=value.toString().split(",");
			if(input.length==10)
			{
				outkey.set(Long.parseLong(input[0]));
				String[] cal=input[9].toString().split("/");
				Date currDate=new Date();
				int currMonth=currDate.getMonth()+1;
				int currYear=currDate.getYear()+1900;
				int result=currYear-Integer.parseInt(cal[2]);
				if(Integer.parseInt(cal[0])>currMonth)
				{
					result--;
				}
				else if(Integer.parseInt(cal[0])==currMonth){
					int currDay=currDate.getDate();
					if(Integer.parseInt(cal[1])>currDay)
						result--;					
				}
				String data=input[1]+","+input[3]+","+input[4]+","+input[5];
				outvalue.set("R:"+data);
				context.write(outkey, outvalue);				
			}
		}
	}
	
	public static class Reducer1 extends Reducer<LongWritable, Text, Text, Text>
	{
		static HashMap<String, String> userData;
		private ArrayList<Text> listA = new ArrayList<Text>();
		private ArrayList<Text> listB = new ArrayList<Text>();
		
		public void setup(Context context) throws IOException{
			Configuration config=context.getConfiguration();
			userData = new HashMap<String, String>();
			String userDataPath =config.get("userdata");
			FileSystem fs = FileSystem.get(config);
			Path path = new Path("hdfs://cshadoop1"+userDataPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line = br.readLine();
			while (line != null) {
				String[] arr = line.split(",");
				if (arr.length == 10) {
					String data = arr[1] + ":" + arr[3]+":"+arr[9];
					userData.put(arr[0].trim(), data);
				}
				line = br.readLine();
			}
		}
		
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			
			listA.clear();
			listB.clear();
			for(Text val: values)
			{
				if(val.toString().charAt(0)=='U')
					listA.add(new Text(val.toString().substring(2)));
				else if(val.toString().charAt(0)=='R')
					listB.add(new Text(val.toString().substring(2)));
			}

			String[] details=null;
			Text C=new Text();
			if(!listA.isEmpty() && !listB.isEmpty())
			{
				for(Text A:listA)
				{
					float age=0;
					float minage=1000;
					String[] friend=A.toString().split(",");
					for(int i=0;i<friend.length;i++)
					{
						if(userData.containsKey(friend[i]))
						{
							String[] ageCal=userData.get(friend[i]).split(":");
							Date curr=new Date();
							int currMonth=curr.getMonth()+1;
							int currYear=curr.getYear()+1900;
							String[] cal=ageCal[2].toString().split("/");
							int result=currYear-Integer.parseInt(cal[2]);
							if(Integer.parseInt(cal[0])>currMonth)
								result--;
							else if(Integer.parseInt(cal[0])==currMonth)
							{
								int currDay=curr.getDate();
								if(Integer.parseInt(cal[1])>currDay)
									result--;
							}
							age=result;
						}
						if(age<minage)
							minage=age;						
					}

					String subdetails="";
					StringBuilder res=new StringBuilder();
					for(Text B:listB)
					{
						details=B.toString().split(",");
						subdetails=B.toString()+","+new Text(new FloatWritable((float) minage).toString());
						res.append(B.toString());
						res.append(",");
						res.append(new Text(new FloatWritable((float) minage).toString()));
					}
					C.set(res.toString());
				}
			}
			context.write(new Text(key.toString()), C);
		}
	}

	public static class MinAgeMapper extends Mapper<LongWritable,Text,MinAgeCustom, Text>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] m=value.toString().split("\t");
			if(m.length==2)
			{
				String line[]=m[1].split(",");
				context.write(new MinAgeCustom(Float.parseFloat(m[0]),Float.parseFloat(line[4])), new Text(m[1].toString()));
			}
		}
	}
	
	public static class MinAgeCustom implements WritableComparable<MinAgeCustom>
	{
		private Float userId;
		private Float age;
		
		public Float getUserID()
		{
			return userId;
		}
		public void setUserId(Float userId)
		{
			this.userId=userId;
		}
		public Float getAge()
		{
			return age;
		}
		public void setAge(Float age)
		{
			this.age=age;
		}
		public MinAgeCustom(Float user, Float age)
		{
			this.userId=user;
			this.age=age;
		}
		public MinAgeCustom(){}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			userId=in.readFloat();
			age=in.readFloat();			
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeFloat(userId);
			out.writeFloat(age);			
		}

		@Override
		public int compareTo(MinAgeCustom o) {
			// TODO Auto-generated method stub
			int result=userId.compareTo(o.userId);
			if(result!=0)
				return result;
			
			return this.age.compareTo(o.age);
		}
		
		public String toString(){
			return userId.toString()+":"+age.toString();
		}
		
		public boolean equals(Object obj)
		{
			if(obj==null)
				return false;
			if(getClass()!=obj.getClass())
				return false;
			final MinAgeCustom other=(MinAgeCustom)obj;
			if(this.userId!=other.userId && (this.userId==null || !this.userId.equals(other.userId)))
				return false;
			if(this.age!=other.age && (this.age==null || !this.age.equals(other.age)))
					return false;
			return true;
		}
		
	}
	
	public class MinAgePartitioner extends Partitioner<MinAgeCustom, Text>{
	    @Override
	    public int getPartition(MinAgeCustom minAge, Text nullWritable, int numPartitions) {
	        return minAge.getAge().hashCode() % numPartitions;
	    }
	}
	
	
	public static class MinAgeBasicCompKeySortComparator extends WritableComparator {

		  public MinAgeBasicCompKeySortComparator() {
				super(MinAgeCustom.class, true);
			}

			@Override
			public int compare(WritableComparable w1, WritableComparable w2) {
				MinAgeCustom key1 = (MinAgeCustom) w1;
				MinAgeCustom key2 = (MinAgeCustom) w2;

				int cmpResult = -1*key1.getAge().compareTo(key2.getAge());
				
				return cmpResult;
			}
		}
	public static class MinAgeBasicGroupingComparator extends WritableComparator {
		  public MinAgeBasicGroupingComparator() {
				super(MinAgeCustom.class, true);
			}

			@Override
			public int compare(WritableComparable w1, WritableComparable w2) {
				MinAgeCustom key1 = (MinAgeCustom) w1;
				MinAgeCustom key2 = (MinAgeCustom) w2;
				return -1*key1.getAge().compareTo(key2.getAge());
			}
		}
	
	public static class MinAgeReducer extends Reducer<MinAgeCustom, Text, Text, Text> 
	{
		TreeMap<String,String> output=new TreeMap<String, String>();        
	
		         
		public void reduce(MinAgeCustom key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			for(Text t:values)
			{
				if(output.size()<10)
				{
					output.put(key.userId.toString(), t.toString());
					 context.write(new Text(), new Text(t.toString()));
				}
			}
		}
	}

	public static void main(String[] args) throws Exception 
	{

		Path outputDirIntermediate1 = new Path(args[3] + "_int1");
		Path outputDirIntermediate2 = new Path(args[4]);
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		conf.set("userdata",otherArgs[0]);
		// get all args
		if (otherArgs.length != 5)
		{
			System.err.println("Usage: JoinExample <inmemory input> <input > <input> <intermediate output> <output>");
			System.exit(2);
		}
		
		Job job = new Job (conf, "join1 ");
		job.setJarByClass(Chain.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),TextInputFormat.class, Mapper1.class );
		MultipleInputs.addInputPath(job, new Path(otherArgs[2]),TextInputFormat.class, Mapper2.class );
		job.setReducerClass(Reducer1.class);		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
	
		FileOutputFormat.setOutputPath(job,outputDirIntermediate1);
		
	    int code = job.waitForCompletion(true)?0:1;
	    Job job1 = new Job(new Configuration(), "join2");
		job1.setJarByClass(Chain.class);
		FileInputFormat.addInputPath(job1, new Path(args[3] + "_int1"));		
		job1.setMapOutputKeyClass(MinAgeCustom.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setPartitionerClass(MinAgePartitioner.class);
		job1.setMapperClass(MinAgeMapper.class);
		job1.setSortComparatorClass(MinAgeBasicCompKeySortComparator.class);
		job1.setGroupingComparatorClass(MinAgeBasicGroupingComparator.class);
		job1.setReducerClass(MinAgeReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job1,outputDirIntermediate2);
		code = job1.waitForCompletion(true) ? 0 : 1;
	
			
	}
}
