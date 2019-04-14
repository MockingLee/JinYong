package neu;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountReducer extends Reducer<Text, IntWritable , Text , IntWritable > {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable v : values){
            sum += v.get();
        }
        context.write(new Text(key.toString()) , new IntWritable(sum));
    }
}
