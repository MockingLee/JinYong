package neu;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GraphBuilder extends Mapper<LongWritable , Text , Text , Text> {

        @Override
        protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String resKey = value.toString().split("\t")[0];
            String resValue = value.toString().split("\t")[1];
            context.write(new Text(resKey) , new Text("0.1#" + resValue));
        }
    }

//    public class Reduce extends Reducer<Text,Text,Text,Text>{
//        @Override
//        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            for (Text t : values){
//                context.write(key , t);
//            }
//        }
//    }



