import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class Following {
        
 public static class FollowingMap extends Mapper<LongWritable,Text,IntWritable, IntWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {

            String token1 = tokenizer.nextToken();
	    String token2 = tokenizer.nextToken();
            context.write( new IntWritable(Integer.parseInt(token1)),new IntWritable(Integer.parseInt(token2))); //(me,following)
        }
    }
 } 
        
 public static class FollowingReduce extends Reducer<IntWritable,IntWritable, IntWritable,  Text> {
    private MapWritable  arry = new MapWritable();
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
	String str = "";
	for(IntWritable val :values){
	    str = str + val.toString()+" ";
	  
	}
	context.write(key, new Text(str));
    }
 }

public static class FollowerMap extends Mapper<LongWritable,Text, IntWritable, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
	String user =  tokenizer.nextToken();
        while (tokenizer.hasMoreTokens()) {
           
            String token1 = tokenizer.nextToken();
            context.write( new IntWritable(Integer.parseInt(token1)),new Text("er" + user)); //(other,follower)
	    context.write( new IntWritable(Integer.parseInt(user)), new Text("ing" + token1)); //(me ,following)
        }
    }
 }

 public static class FollowerReduce extends Reducer<IntWritable,Text, IntWritable,  Text> {
   
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
        
        String str = " [";
	String str2 = " [";
	

        for(Text val :values){
		String  tag =  val.toString();
		if(tag.charAt(0)=='e'){
	    		
			str = str + tag.substring(2) + " ";
           	}
		else {
			str2 = str2 + tag.substring(3) + " ";
		}
	}
        str+="]-->follower list";
	str2+="]-->following list";
	 context.write(key, new Text(str2));
        context.write(key, new Text(str));
    }
 }

        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
    Job job = new Job(conf, "hello");
        
    job.setMapperClass(FollowingMap.class);
    job.setReducerClass(FollowingReduce.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
   
    job.setJarByClass(Following.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);



    Configuration conf2 = new Configuration();

    Job job2 = new Job(conf2, "hello");


    job2.setMapperClass(FollowerMap.class);
    job2.setReducerClass(FollowerReduce.class);

    job2.setMapOutputKeyClass(IntWritable.class);
    job2.setMapOutputValueClass(Text.class);

    job2.setOutputKeyClass(IntWritable.class);
    job2.setOutputValueClass(Text.class);

    job2.setJarByClass(Following.class);

    job2.setInputFormatClass(TextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job2, new Path(args[1]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));

    job2.waitForCompletion(true);

   
 }
        
}
