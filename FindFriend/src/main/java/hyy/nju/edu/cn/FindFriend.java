package hyy.nju.edu.cn;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
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
	public static class ListReverseMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context
	            ) throws IOException, InterruptedException {
			String line = value.toString(); 
			String[] userAndfriends = line.split(", "); 
			String user =  userAndfriends[0];	//user name
			String[] friends = userAndfriends[1].split("\\s+"); //user friends
			for(String friend:friends) {
				context.write(new Text(friend), new Text(user)); //key: being followed by value
			}
		}
	}
	/**
	 * First reduce: get the reverse friend list of friend person1, person2, ...
	 *
	 */
	public static class ListReverseReducer extends Reducer<Text,Text,Text,Text> {
		 public void reduce(Text friend, Iterable<Text> users, Context context) 
				 throws IOException, InterruptedException {
			 StringBuffer list = new StringBuffer();
			 for(Text user: users) {
				 list.append(user).append(",");
			 }
			 String friendlist = list.toString();
			 context.write(friend, new Text(friendlist.substring(0,friendlist.length()-1)));
		 }
	}
	/*
	 *  Second Map: 
	 *  input: friend person1,person2,person3
	 *  output: [person1,person2] friend
	 *  		[person2,person3] friend
	 */
	public static class CommonRegMapper extends Mapper<Object, Text, Text, Text> {
		@Override
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] friendAndusers = line.split("\t");
            String friend = friendAndusers[0];
            String[] users = friendAndusers[1].split(",");
            Arrays.sort(users);// sort the person list by rank
            for (int i = 0; i < users.length - 2; i++) {    
                for (int j = i + 1; j < users.length - 1; j++) {
                    context.write(new Text("[" + users[i] + "," + users[j] + "],"), new Text(friend));
                }
            }//iterate the person-pair group to find their common friends
		}
	}
	/*
	 * Second reducer: [person1, person2],[friend1,friend2]
	 */
	public static class CommonGroupReducer extends Reducer<Text, Text, Text, Text> {
		 public void reduce(Text friendpair, Iterable<Text> commonfriends, Context context) 
				 throws IOException, InterruptedException {
			 StringBuffer friendlist = new StringBuffer();
	            for (Text friend : commonfriends) {
	            	friendlist.append(friend).append(","); 
	            }
	            String commonfriendlist = friendlist.toString();
	            context.write(friendpair, new Text("["+commonfriendlist.substring(0,commonfriendlist.length()-1)+"]"));
		 }
	}
	
	
    public static void main( String[] args ) throws 
    IOException, ClassNotFoundException, InterruptedException
    {	
    	boolean exit = false;
    	Configuration conf = new Configuration();
    	GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        Path tempDir1 = new Path("findfriend-temp-output");
        if (remainingArgs.length < 2) {
            System.err.println("Usage: sharedfriends <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job1 = Job.getInstance(conf, "FindFriend-job-1: Reverse Friendlist");
        job1.setJarByClass(FindFriend.class);
        job1.setMapperClass(ListReverseMapper.class);
        job1.setReducerClass(ListReverseReducer.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, tempDir1);
        if(job1.waitForCompletion(true)){
        	Job job2 = Job.getInstance(conf, "findfriend-job2");
        	job2.setJarByClass(FindFriend.class);
        	job2.setInputFormatClass(SequenceFileInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);	
			job2.setOutputKeyClass(Text.class);
		    job2.setOutputValueClass(Text.class);
		    job2.setMapperClass(CommonRegMapper.class); 
		    job2.setReducerClass(CommonGroupReducer.class);
		    
		    FileInputFormat.addInputPath(job2, tempDir1);
		    FileOutputFormat.setOutputPath(job2,new Path(args[1]));
		    exit = job2.waitForCompletion(true);
        }
        FileSystem.get(conf).deleteOnExit(tempDir1);
		if(exit) System.exit(1);
		System.exit(0);
    }
}
