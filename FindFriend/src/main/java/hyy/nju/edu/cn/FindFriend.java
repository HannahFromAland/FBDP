package hyy.nju.edu.cn;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

/**
 * Find Shared friend list
 * 
 */
public class FindFriend{
	 /**
     * First map:reverse the input to be [friend, person].
     * 
     */
	public static class ListReverseMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
		public void map(IntWritable key, IntWritable value, Context context
	            ) throws IOException, InterruptedException {
			String line = value.toString(); 
			String[] userAndfriends = line.split(":"); 
			int user =  Integer.valueOf(userAndfriends[0]).intValue();	//user name
			String[] friends = userAndfriends[1].split("\\s+"); //user friends
			for(String friend:friends) {
				int friendnum = Integer.valueOf(friend).intValue();
				context.write(new IntWritable(friendnum), new IntWritable(user)); //key: being followed by value
			}
		}
	}
	/**
	 * First reduce: get the reverse friend list of friend person1, person2, ...
	 *
	 */
	public static class ListReverseReducer extends Reducer<IntWritable, IntWritable,  IntWritable,Text> {
		 public void reduce(IntWritable friendnum, Iterable<IntWritable> users, Context context) 
				 throws IOException, InterruptedException {
			 StringBuffer s = new StringBuffer();
			 for(IntWritable user: users) {
				 if (s.length() != 0) {
	                    s.append(",");
	                }
				 String username = user+"";
				 s.append(username);
			 }
			 context.write(friendnum, new Text(s.toString()));
		 }
	}
	//public static class CommonRegMapper extends Mapper<Object, Text, Text, IntWritable> {}
	//public static class CommonGroupReducer extends Reducer<Text, IntWritable, Text, IntWritable> {}
	
	
    public static void main( String[] args ) throws 
    IOException, ClassNotFoundException, InterruptedException
    {
    	Configuration conf = new Configuration();
    	GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if (remainingArgs.length < 2) {
            System.err.println("Usage: sharedfriends <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job1 = Job.getInstance(conf, "FindFriend-job-1: Reverse Friendlist");
        job1.setJarByClass(FindFriend.class);
        job1.setMapperClass(ListReverseMapper.class);
        job1.setReducerClass(ListReverseReducer.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        
        job1.waitForCompletion(true);
        System.exit(0);
    }
}
