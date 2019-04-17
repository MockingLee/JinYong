package neu;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LPAReorganize {

    public static class LPAMap extends Mapper<LongWritable , Text,Text , Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("\t");
            String key_ = strs[0].split("#")[0];
            String value_ = strs[0].split("#")[1] + "#" + strs[1];

            context.write(new Text(strs[0].split("#")[0]) , new Text(strs[0].split("#")[1] + "#" + strs[1]));
        }
    }

    public static class LPAReduce extends Reducer<Text , Text ,Text , Text>{
        private int id = 1;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text t : values){
                context.write(new Text(  id + "$" + t.toString().split("#")[0]) , new Text(t.toString().split("#")[1] + "#" + t.toString().split("#")[2]));
                System.out.println(id);
            }

            id++;
        }
    }

}
