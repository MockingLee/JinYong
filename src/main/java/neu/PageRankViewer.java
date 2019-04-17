package neu;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class PageRankViewer {

    public static class PRVMap extends Mapper<LongWritable , Text, SortClass , Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("\t");
            context.write(new SortClass(Float.parseFloat(strs[1].split("#")[0])) , new Text(strs[0]));
        }
    }

    public static class PRVReduce extends Reducer<SortClass , Text ,Text , Text>{
        @Override
        protected void reduce(SortClass key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet<String> set = new HashSet<>();
            for (Text t : values){
                if (!set.contains(t.toString()))
                    context.write(new Text(key.getPr().toString()) , new Text(t.toString()));
            }
        }
    }


}
