import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.hash.Hash;


public class TopTenCommon{

    public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
        	Text word = new Text();
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

 
    public static class Reducer1 extends org.apache.hadoop.mapreduce.Reducer<Text,Text,Text,IntWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> mapkey = new HashMap<String, Integer>();
            int count = 0;
            for(Text Friend : values){
                List<String> datavalue = Arrays.asList(Friend.toString().split(","));
                for(String friend: datavalue){
                    if(mapkey.containsKey(friend)){
                        count+=1;
                    }else{
                        mapkey.put(friend, 1);
                    }
                }
            }
            context.write(key, new IntWritable(count));
        }
    }
    
    public static class Mapper2 extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final static IntWritable iw = new IntWritable(1);
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(iw, value);
        }
    }

    
    
    public static class Reducer2 extends org.apache.hadoop.mapreduce.Reducer<IntWritable, Text, Text, IntWritable>{

        static class ValueComparator implements Comparator {
            HashMap map;
            public ValueComparator(HashMap map) {
                this.map = map;
            }
            public int compare(Object key1, Object key2) {
                Integer num1 = (Integer) map.get(key1);
                Integer num2 = (Integer) map.get(key2);
                if (num1 == num2 || num2 > num1) {
                    return 1;
                } else {
                    return -1;
                }
            }
        }

        public void reduce(IntWritable key, Iterable<Text> values, Context context)throws IOException, InterruptedException{
            HashMap<String, Integer> mapkey = new HashMap<String, Integer>();
            for(Text value: values){
                String[] line = value.toString().split("\t");
                String s = line[0];
                Integer i = Integer.parseInt(line[1]);
                
                mapkey.put(s, i);
            }
         
            TreeMap<String, Integer> sorted =  new TreeMap<String, Integer>(new ValueComparator(mapkey));
            sorted.putAll(mapkey);
            int count = 0;
            for(Entry<String, Integer> entry: sorted.entrySet()){
                
                context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
                count++;
               
                if(count==10){
                    break;
                }
            }
        }
    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
    	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 3) {
			System.err.println("Usage: MutualFriends <in> <out>");
			System.exit(2);
		}
        Job job = Job.getInstance(conf, "Topten1");
        job.setJarByClass(TopTenCommon.class);
        job.setMapperClass(Mapper1.class);
        job.setReducerClass(Reducer1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        if (job.waitForCompletion(true)) {
            Configuration conf1 = new Configuration();
            Job job2 = new Job(conf1, "TopTen2");
            job2.setJarByClass(TopTenCommon.class);
            job2.setMapperClass(Mapper2.class);
            job2.setReducerClass(Reducer2.class);
            job2.setInputFormatClass(TextInputFormat.class);
            job2.setMapOutputKeyClass(IntWritable.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(IntWritable.class);
            
            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}