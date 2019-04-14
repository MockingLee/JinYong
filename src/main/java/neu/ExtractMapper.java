package neu;

import org.ansj.domain.Result;
import org.ansj.splitWord.analysis.NlpAnalysis;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.ansj.domain.Term;
import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.ToAnalysis;

import java.io.*;
import java.util.HashSet;
import java.util.List;

public class ExtractMapper extends Mapper<LongWritable, Text, Text, Text> {
    static HashSet<String> nameSet = new HashSet<>();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        String nameListPath = "./person/jinyong_all_person.txt";
        FileInputStream inputStream = new FileInputStream(nameListPath);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        String str = null;
        while((str = bufferedReader.readLine()) != null)
        {
            nameSet.add(str);
            DicLibrary.insert(DicLibrary.DEFAULT, str);
        }
        //close
        inputStream.close();
        bufferedReader.close();

    }


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        Result result = NlpAnalysis.parse(line);
        List<Term> termList = result.getTerms();
        String str = "";
        for(Term term:termList){
            //System.out.println(context.getInputSplit());
            //System.out.println(term.getName());
            if (nameSet.contains(term.getName())){
                //System.out.println(term.getName());
                str += term.getName()+ " ";
            }
        }
        if (!str.equals(""))
            context.write(new Text(key.toString()) , new Text(str));

    }




}

