import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * Created by hadoop on 17-4-22.
 */
public class NewPartitioner extends HashPartitioner<Text, IntWritable> {
    public int getPartion(Text key, IntWritable value, int numReduceTasks){
        String term = new String();
        term = key.toString().split("#")[0];
        return super.getPartition(new Text(term), value, numReduceTasks);
    }
}
