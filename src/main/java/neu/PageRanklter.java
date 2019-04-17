package neu;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PageRanklter {

    public static class PRMap extends Mapper<LongWritable , Text , Text , Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] strs = value.toString().split("\t");
            String pr = strs[1].split("#")[0];
            context.write(new Text(strs[0]) , new Text("#" + strs[1].split("#")[1]));
            for (String s : strs[1].split("#")[1].split(";")){
                context.write(new Text(s.split(":")[0]) , new Text(pr + "*" + s.split(":")[1]));
            }
        }
    }

    public static class PRReduce extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String tail = "";
            float sum = 0;
            for (Text t : values){
                if (t.toString().startsWith("#"))
                    tail = t.toString();
                else {
                    sum += Float.parseFloat(t.toString().split("\\*")[0]) * Float.parseFloat(t.toString().split("\\*")[1]);
                }
            }
            context.write(key , new Text(sum + tail));
        }
    }

}
