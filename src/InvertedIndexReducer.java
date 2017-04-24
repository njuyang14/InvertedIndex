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
    /*private Text word1 = new Text();//
    private Text word2 = new Text();
    String temp = new String();
    static Text CurrentItem = new Text(" ");
    static List<String> postingList = new ArrayList<String>();*/
    private String term = new String();//临时存储word#filename中的word
    private String last = " ";//临时存储上一个word
    private int countItem;//统计word一共出现次数
    private int countDoc;//统计有该word出现的文件的数目
    private StringBuilder out = new StringBuilder();//临时存储输出的value部分
    private float f;//临时计算平均出现频率

    /**reduce()输出key-value格式：term <doc1, num>...<total, sum>*/
    public void reduce(Text key, Iterable<IntWritable> values, Context context)/**<word#doc>相同的迭代器*/
        throws IOException, InterruptedException {
        /*int sum = 0;
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
        }*/
        term = key.toString().split("#")[0];//获取word
        if (!term.equals(last)) {//此次word与上次不一样，则将上次进行处理并输出
            if (!last.equals(" ")) {//排除第一次reduce情况
                out.setLength(out.length() - 1);//删除value部分最后的;符号
                f = (float) countItem / countDoc;//计算平均出现次数
                context.write(new Text(last), new Text(String.format("%.2f,%s", f, out.toString())));//value部分拼接后输出
                countItem = 0;//以下清除变量，初始化计算下一个word
                countDoc = 0;
                out = new StringBuilder();
            }
            last = term;//更新word，为下一次做准备
        }
        int sum = 0;//累加相同word在某个doc中出现次数
        for (IntWritable val : values) {
            sum += val.get();
        }
        out.append(key.toString().split("#")[1] + ":" + sum + ";");//将filename:NUM; 临时存储
        countItem += sum;
        countDoc += 1;
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        /*StringBuilder out = new StringBuilder();
        long count = 0;
        for(String p: postingList){
            out.append(p);
            out.append(";");
            count += Long.parseLong(p.substring(p.indexOf(",")+1,p.indexOf(">")));
        }
        out.append("total,"+count+">.");
        if(count>0){
            context.write(CurrentItem,new Text(out.toString()));
        }*/
        out.setLength(out.length() - 1);
        f = (float) countItem / countDoc;
        context.write(new Text(last), new Text(String.format("%.2f,%s", f, out.toString())));
    }
}
