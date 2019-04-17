package neu;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LPAInit {

    public static class LPAMap extends Mapper<LongWritable, Text,Text,Text>{
        private int unique_id = 0;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("\t");
            context.write(new Text(String.valueOf(unique_id) + "#" + strs[0] + "#" + strs[1].split("#")[0]) , new Text(strs[1]));
            unique_id ++;
        }
    }

}
