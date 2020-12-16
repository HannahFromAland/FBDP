package join;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.ArrayList;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReduceJoin {
    public static class JoinMapper extends Mapper<LongWritable, Text, Text, InfoBean> {
        InfoBean bean = new InfoBean();
        Text t = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] field = line.split(",");
            FileSplit split = (FileSplit) context.getInputSplit();
            String name = split.getPath().getName();

            int user_id= 0;
            if(name.endsWith("log_format1.csv") && field.length ==7) {
                bean.set(Integer.parseInt(field[0]),field[1]+" "+field[2]+" "+field[3]+" "+field[4]+" "+field[5]+" "+field[6], "","log");
                user_id= Integer.parseInt(field[0]);
            }else if(name.endsWith("info_format1.csv") && field.length ==3){
                bean.set(Integer.parseInt(field[0]), " ",field[1]+" "+field[2] , "info");
                user_id= Integer.parseInt(field[0]);
            }
            t.set(String.valueOf(user_id));
            context.write(t, bean);
        }
    }
    public static class TestReducer extends Reducer<Text,InfoBean,Text,InfoBean>{

        protected void reduce(Text pid, InfoBean beans, Context context)
                throws IOException, InterruptedException {
            context.write(pid, beans);
        }
    }
    public static class JoinReducer extends Reducer<Text, InfoBean, Text,NullWritable>{
        @Override
        protected void reduce(Text pid, Iterable<InfoBean> beans, Context context)
                throws IOException, InterruptedException {
            ArrayList<String> infBeans = new ArrayList<>();
            ArrayList<String> loggerBeans = new ArrayList<>();

            for (InfoBean infoBean : beans) {
                //information
                if("info".equals(infoBean.getFlag())) {
                        infBeans.add(infoBean.toInfo());

                }else {
                    loggerBeans.add(infoBean.toLog());
                }
            }
            // join
            for(int i=0;i<loggerBeans.size();i++){
                for(int j=0;j<infBeans.size();j++){
                    context.write(new Text(loggerBeans.get(i)+" "+infBeans.get(j)), NullWritable.get() );
                }
            }


        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(ReduceJoin.class);

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InfoBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}


