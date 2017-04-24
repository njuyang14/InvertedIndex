import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.net.URI;

/**
 * Created by hadoop on 17-4-22.
 * import /share/hadoop/common/*common.jar
 * import /share/hadoop/mapreduce/*client-core.jar
 * import /share/hadoop/mapreduce/lib/*.jar
 */
public class InvertedIndex {
    public static void main(String []args){
        //add cacheFile

        //set job
        try {
            Configuration conf=new Configuration();//从hadoop配置文件中读取参数
            //DistributedCache.addCacheFile(new URI("./stop-words.txt"),conf);
            //从命令行读取参数
            String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();

            if(otherArgs.length!=2){
                System.out.println("Usage:invertIndex <in> <out>");
                System.exit(2);
            }

            Job job = new Job(conf, "invertIndex");
            job.setJarByClass(InvertedIndex.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(InvertedIndexMapper.class);
            job.setCombinerClass(SumCombiner.class);
            job.setPartitionerClass(NewPartitioner.class);
            job.setReducerClass(InvertedIndexReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
