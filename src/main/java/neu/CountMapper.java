package neu;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

public class CountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        HashSet<String> set = new HashSet<>();
        StringTokenizer tkn = new StringTokenizer(value.toString());
        while (tkn.hasMoreTokens()){
            set.add(tkn.nextToken());
        }
        System.out.println(value.toString());
        for (String name1 : set){
            for (String name2 : set){
                if (!name1.equals(name2)){
                    //System.out.println(name1 + "_" + name2);
                    context.write(new Text(name1 + "_" + name2) , new IntWritable(1));
                }

            }
        }
    }
}
