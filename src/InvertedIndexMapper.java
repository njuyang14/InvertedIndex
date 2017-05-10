import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

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
    //private Path[] localFiles;
    private Configuration conf = HBaseConfiguration.create();

    public void setup(Context context)throws IOException,InterruptedException{
        stopwords = new TreeSet<String>();
        HTable table = new HTable(conf, "stopwords");
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        for(Result s:resultScanner){
            stopwords.add(Bytes.toString(s.getRow()));
        }
        table.close();
    }

    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException{
        // default RecordReader: LineRecordReader; key: line offset; value: line string
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName().toString().split(".txt|.TXT")[0];
        String temp = new String();
        String line = value.toString().toLowerCase();
        StringTokenizer itr = new StringTokenizer(line);
        for(;itr.hasMoreTokens();){
            temp = itr.nextToken();
            if(!stopwords.contains(temp)){
                Text word = new Text();
                word.set(temp+"#"+fileName);//format:<word#doc1>
                context.write(word,new IntWritable(1));//<key:word#doc,value:1>
            }
        }
    }
}
