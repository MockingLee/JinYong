package neu;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.LinkedList;

public class BuildReducer extends Reducer<Text , Text, Text , Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        LinkedList<Integer> freqList = new LinkedList<>();
        LinkedList<String> nameList = new LinkedList<>();

        int sum = 0;

        for (Text value : values){
            String[] strs = value.toString().split(":");
            int freq = Integer.valueOf(strs[1]);
            sum += freq;
            freqList.add(freq);
            nameList.add(strs[0]);
        }

        StringBuilder strs = new StringBuilder("");

        for (int i = 0; i < nameList.size(); i++) {
            float freq = freqList.get(i)/(float)sum;
            strs.append(nameList.get(i) + ":" + freq + ";");
        }
        context.write(new Text(key) , new Text(strs.toString()));
    }
}
