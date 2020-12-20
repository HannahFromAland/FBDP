package PopularItem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashSet;

public class PopMerYoung {
    public static class CountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        IntWritable one = new IntWritable(1);
        Text t = new Text();
        HashSet<String> cartSet= new HashSet<String>();
        HashSet<String> starSet= new HashSet<String>();
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] field = line.split(" ");
            FileSplit split = (FileSplit) context.getInputSplit();

            String merchant_id = field[3];
            String user_id = field[0];
            String age_range = field[7];
            if("1".equals(age_range) || "2".equals(age_range) || "3".equals(age_range)){
                if("2".equals(field[6])){
                    t.set(String.valueOf(merchant_id));
                    context.write(t, one);
                }
                if("1".equals(field[6]) ){
                    if(!cartSet.contains(user_id)){
                        cartSet.add(user_id);
                        t.set(String.valueOf(merchant_id));
                        context.write(t, one);
                    }
                }
                else if("3".equals(field[6]) ){
                    if(!starSet.contains(user_id)){
                        starSet.add(user_id);
                        t.set(String.valueOf(merchant_id));
                        context.write(t, one);
                    }
                }
            }

        }
    }
    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text	key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException {
            int sum = 0;
            for(IntWritable val:values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key,result);
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
        int rank=0;
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {


            for (Text val : values) {
                if(rank>=100) {
                    break;
                }
                rank++;
                countrank.set(rank);
                result.set(val.toString());
                String pair = "Merchant:"+result+" Popularity:"+key;

                context.write(countrank, new Text(pair));
            }
        }
    }


    public static void main(String[] args) throws Exception {
        boolean exit = false;
        Path tempDir1 = new Path("AllItem-temp-output");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"sumItem");
        job.setJarByClass(AllItem.class);

        job.setMapperClass(CountMapper.class);
        job.setReducerClass(SumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, tempDir1);
        if(job.waitForCompletion(true)){
            Job job2 = Job.getInstance(conf, "sortItem");
            job2.setJarByClass(AllItem.class);
            job2.setInputFormatClass(SequenceFileInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);

            job2.setMapOutputKeyClass(IntWritable.class);
            job2.setMapOutputValueClass(Text.class);

            job2.setOutputKeyClass(IntWritable.class);
            job2.setOutputValueClass(Text.class);

            job2.setMapperClass(InverseMapper.class);
            job2.setNumReduceTasks(1);
            job2.setSortComparatorClass(IntWritableDescendingComparator.class);
            job2.setReducerClass(SortIntValueReduce.class);

            FileInputFormat.addInputPath(job2, tempDir1);
            FileOutputFormat.setOutputPath(job2,new Path(args[1]));
            exit = job2.waitForCompletion(true);

        }
        FileSystem.get(conf).deleteOnExit(tempDir1);
        if(exit) System.exit(1);
        System.exit(0);
    }
}
