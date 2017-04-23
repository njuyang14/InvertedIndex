import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hadoop on 17-4-22.
 */
public class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text>{
    private Text word1 = new Text();
    private Text word2 = new Text();
    String temp = new String();
    static Text CurrentItem = new Text(" ");
    static List<String> postingList = new ArrayList<String>();
    /**reduce()输出key-value格式：term <doc1, num>...<total, sum>*/
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
        int sum = 0;
        word1.set(key.toString().split("#")[0]);
        temp = key.toString().split("#")[1];
        for(IntWritable val: values){
            sum += val.get();
        }
        word2.set("<"+temp+","+sum+">");
        if(!CurrentItem.equals(word1) && !CurrentItem.equals(word2)){
            StringBuilder out = new StringBuilder();
            long count = 0;
            for(String p: postingList){
                out.append(p);
                out.append(";");
                count += Long.parseLong(p.substring(p.indexOf(",")+1,p.indexOf(">")));
            }
            out.append("total,"+count+">.");
            if(count>0){
                context.write(CurrentItem,new Text(out.toString()));
            }
            postingList.add(word2.toString());
        }
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        StringBuilder out = new StringBuilder();
        long count = 0;
        for(String p: postingList){
            out.append(p);
            out.append(";");
            count += Long.parseLong(p.substring(p.indexOf(",")+1,p.indexOf(">")));
        }
        out.append("total,"+count+">.");
        if(count>0){
            context.write(CurrentItem,new Text(out.toString()));
        }
    }
}
