import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

/**
 * Created by hadoop on 17-4-22.
 * import /share/hadoop/common/*common.jar
 * import /share/hadoop/mapreduce/*client-core.jar
 * import /share/hadoop/mapreduce/lib/*.jar
 */
public class InvertedIndexMapper extends Mapper<Object, Text, Text, IntWritable>{
    private Set<String> stopwords;
    private Path[] localFiles;

    public void setup(Context context)throws IOException,InterruptedException{
        stopwords = new TreeSet<String>();
        Configuration conf = context.getConfiguration();
        localFiles = DistributedCache.getLocalCacheFiles(conf);
        for(int i = 0; i < localFiles.length; i++){
            String line;
            BufferedReader br = new BufferedReader(new FileReader(localFiles[i].toString()));
            while((line = br.readLine()) != null){
                StringTokenizer itr = new StringTokenizer(line);
                while(itr.hasMoreTokens()){
                    stopwords.add(itr.nextToken());
                }
            }
        }
    }

    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException{
        // default RecordReader: LineRecordReader; key: line offset; value: line string
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        String temp = new String();
        String line = value.toString().toLowerCase();
        StringTokenizer itr = new StringTokenizer(line);
        for(;itr.hasMoreTokens();){
            temp = itr.nextToken();
            if(!stopwords.contains(temp)){
                Text word = new Text();
                word.set(temp+"#"+fileName);//format:<word#doc1>
                context.write(word,new IntWritable(1));
            }
        }
    }
}
