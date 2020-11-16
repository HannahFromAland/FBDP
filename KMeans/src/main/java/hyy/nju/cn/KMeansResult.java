package hyy.nju.cn;
import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static hyy.nju.cn.KMeans.Kmeansmain;

public class KMeansResult {
    private static int k = 0;
    private static ArrayList<Cluster> kClusters;

    /*mapper将每个点作为cluster输出是为了在combiner里做提前处理（提前求一次中心点）
     * 因为combiner的输出必须和mapper一样，而在combiner中计算局部中心点时，必须输出用于计算的点的个数以在reducer中进一步算均值
     * 因此如果将点直接作为Instance进行输出，则还需要额外输出点的个数信息，不如直接作为cluster输出
     * */
    public static class KMeansResultMapper extends Mapper<Object, Text, IntWritable, Instance>{
        IntWritable map_key = new IntWritable();
        Instance map_value;

        /*初始化所有的簇*/
        public void setup(Context context) throws IOException{
            k = context.getConfiguration().getInt("k",0);
            kClusters = new ArrayList<Cluster>();
            String cluster_path = context.getConfiguration().get("clusterPath");
            FileSystem hdfs = FileSystem.get(context.getConfiguration());
            FSDataInputStream clusterFile = hdfs.open(new Path(cluster_path));
            //BufferedReader in = new BufferedReader(new InputStreamReader(clusterFile, "UTF-8"));
            Scanner scan = new Scanner(clusterFile);
            String line = null;
            while(scan.hasNext()){
                line = scan.nextLine();
                Cluster c = new Cluster(line);
                kClusters.add(c);
            }
            assert (kClusters.size() == k);
            scan.close();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Instance ins = new Instance(value.toString());
            int id = ins.chooseNearestCluster(kClusters);
            map_key.set(id);
            map_value = ins;
            context.write(map_key, map_value);
        }
    }


    public static void main(String[] args) throws Exception {
        //0:k 1:inputPath 2:outputPath(cluster path) 3:iteration times
        Kmeansmain(args);
        int iter_times = Integer.parseInt(args[3]);
        int k = Integer.parseInt(args[0]);
        String inputPath = args[1];
        String clusterPath = args[2];
        Configuration conf = new Configuration();
        conf.setInt("k",k);
        conf.set("clusterPath",clusterPath + "cluster" + iter_times + "/" + "part-r-00000");
        Job job = Job.getInstance(conf, "KMeansResult");
        job.setJarByClass(KMeansResult.class);
        job.setMapperClass(KMeansResultMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Instance.class);
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(clusterPath + "result"));
        System.out.println(job.waitForCompletion(true) ? "success":"failure");

    }
}

