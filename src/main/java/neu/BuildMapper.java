package neu;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BuildMapper extends Mapper<LongWritable , Text , Text , Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] strs = value.toString().split("\t");
        String[] names = strs[0].split("_");
        context.write(new Text(names[0]) , new Text(names[1] + ":" + strs[1]));

    }
}
