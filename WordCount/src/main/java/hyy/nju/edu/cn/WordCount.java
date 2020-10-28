package hyy.nju.edu.cn;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

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


public class WordCount {

    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
    	
    	String regex = "`~!@#$%^&*()_+-=[]{}\\|;'':\",./<>?0123456789"; //remove all punctuation and number
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Set<String> StopWords = new HashSet<String>();
        private BufferedReader bufferreader;
        private Configuration conf;
        /**
         * read in the stopword file
        */
        private void readStopWordFile(String path) {
        	try {
                bufferreader = new BufferedReader(new FileReader(path));
                String stopword = null;
                while ((stopword= bufferreader.readLine()) != null) {
                  StopWords.add(stopword);
                }
              } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                    + StringUtils.stringifyException(ioe));
              }
        }
        /**
         * read stopwords into hashset
         */
        @Override
        public void setup(Context context) throws IOException,
        InterruptedException {			
        	conf = context.getConfiguration();
        	if (conf.getBoolean("wordcount.skip", false)) {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
                for (URI patternsURI : patternsURIs) {
                  Path patternsPath = new Path(patternsURI.getPath());
                  String patternsFileName = patternsPath.getName().toString();
                  readStopWordFile(patternsFileName);
                }
              }
		}  

    public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {
    		String line = value.toString().toLowerCase();
    		line = line.replaceAll(regex, " "); //remove all punctuation
    		
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
            	String s=itr.nextToken();
            	if(s.length()>=3 && StopWords.contains(s)==false) {
            		word.set(s);
            		context.write(word, one);
            	}
                
            }
        }
  
    

    
    }
    public static class CountSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    	private IntWritable result = new IntWritable();

    	   public void reduce(Text key, Iterable<IntWritable> values, 
    	                      Context context
    	                      ) throws IOException, InterruptedException {
    	     int sum = 0;
    	     for (IntWritable val : values) {
    	       sum += val.get();
    	     }
    	     result.set(sum);
    	     context.write(key, result);

    	   }
        
        
    }

    /*InverseMapper
    public class InverseMapper<K, V> extends Mapper<K,V,V,K> {  


        // The inverse function.  Input keys and values are swapped.
        @Override  
        public void map(K key, V value, Context context  
                        ) throws IOException, InterruptedException {  
          context.write(value, key);  
        }
    }
*/

    private static class IntWritableDescendingComparator extends IntWritable.Comparator {
       
	      public int compare(WritableComparable a, WritableComparable b) {
	    	  return -super.compare(a, b);
	      }
	      
	      public int compare(byte[] b1, int s1, int l1, 
	    		  			 byte[] b2, int s2, int l2) {
	          return -super.compare(b1, s1, l1, b2, s2, l2);
	      }
	}
    //key: count value: word
    public static class SortIntValueReduce extends Reducer<IntWritable, Text, IntWritable,Text> {

    	private static IntWritable countrank = new IntWritable();
    	private Text result = new Text();
    	@Override
    	public void reduce(IntWritable key, Iterable<Text> values, Context context) 
    			throws IOException, InterruptedException {
    		int rank=1;
    	for (Text val : values) {
    		while(rank<=100) {
    			countrank.set(rank);
    			String pair = key.toString()+val.toString();
    			result.set(pair);
    			context.write(countrank, result);
    			rank++;
    		}
    		
    	
    	
    			}
    		}
    	}


    public static void main(String[] args) throws 
    IOException, ClassNotFoundException, InterruptedException {
    	boolean exit = false;
        Configuration conf = new Configuration();
        
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        Path tempDir1 = new Path("wordcount1-temp-output");
        //Path tempDir2 = new Path("wordcount2-temp-output");
        if ((remainingArgs.length != 2) && (remainingArgs.length != 4)) {
          System.err.println("Usage: wordcount <in> <out> [-skip StopWordsList]");
          System.exit(2);
        }
        Job job1 = Job.getInstance(conf, "wordcountjob1-countsum");
        // set path for jar
        job1.setJarByClass(WordCount.class);
        // set mapper, reducer and combiner
        job1.setMapperClass(WordCountMapper.class);
        //job1.setCombinerClass(CountSumReducer.class);
        job1.setReducerClass(CountSumReducer.class);
        // set the final type of output  
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        
        //set the path of stopwords
        for(int i=0;i<remainingArgs.length;i++)
	    {
			if("-skip".equals(remainingArgs[i]))
			{
				job1.addCacheFile(new Path(remainingArgs[++i]).toUri());
		        job1.getConfiguration().setBoolean("wordcount.skip", true);
				System.out.println(remainingArgs[i]);
			}			
		}
        
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, tempDir1);
        if(job1.waitForCompletion(true)){
        	Job job2 = Job.getInstance(conf, "wordcountjob2-countsort");
        	job2.setJarByClass(WordCount.class);
			//set format of input-output
			job2.setInputFormatClass(SequenceFileInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);		
			
			//set class of output's key-value
			job2.setOutputKeyClass(IntWritable.class);
		    job2.setOutputValueClass(Text.class);
		    
		    job2.setMapperClass(InverseMapper.class);    
		    job2.setNumReduceTasks(1);
		    job2.setSortComparatorClass(IntWritableDescendingComparator.class);
		    job2.setReducerClass(SortIntValueReduce.class);
		    
		    FileInputFormat.addInputPath(job2, tempDir1);
		    FileOutputFormat.setOutputPath(job2,new Path(args[1]));
        }
        FileSystem.get(conf).deleteOnExit(tempDir1);
		if(exit) System.exit(1);
		System.exit(0);
    }
    
	
   

}
