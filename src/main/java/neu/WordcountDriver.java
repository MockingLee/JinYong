package neu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import neu.PageRanklter;
import java.io.IOException;

/**
 * 相当于一个yarn集群的客户端
 * 需要在此封装我们的mr程序的相关运行参数，指jar包
 * 最后提交给yarn
 */
public class WordcountDriver {
    static {
        try {
            System.load("C:/Users/Lee/Desktop/My Document/hadoop-2.7.6/bin/hadoop.dll");
        } catch (UnsatisfiedLinkError e) {
            System.err.println("Native code library failed to load.\n" + e);
            System.exit(1);
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\Users\\Lee\\Desktop\\My Document\\hadoop-2.7.6");
        if (args == null || args.length == 0) {
            return;
        }

        //该对象会默认读取环境中的 hadoop 配置。当然，也可以通过 set 重新进行配置
        boolean res = false;

//        for (int i = 0; i < 10; i++) {
//            res = PageRanklterJob(i);
//        }
//        for (int i = 0; i < 6; i++) {
//            res = LPAIterationJob(i);
//        }
        res = LPAReorganizeJob();



        System.exit(res ? 0 : 1);
    }

    public static boolean job1() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        //job 是 yarn 中任务的抽象。
        Job job = Job.getInstance(conf);

        /*job.setJar("/home/hadoop/wc.jar");*/
        //指定本程序的jar包所在的本地路径
        job.setJarByClass(WordcountDriver.class);

        //指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(ExtractMapper.class);
        job.setReducerClass(ExtractReducer.class);

        //指定mapper输出数据的kv类型。需要和 Mapper 中泛型的类型保持一致
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //指定最终输出的数据的kv类型。这里也是 Reduce 的 key，value类型。
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path("./input"));
        //指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path("./Job1Output"));

        //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
        /*job.submit();*/
        boolean res = job.waitForCompletion(true);
        return res;
    }

    public static boolean job2() throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();

        //job 是 yarn 中任务的抽象。
        Job job = Job.getInstance(conf);

        /*job.setJar("/home/hadoop/wc.jar");*/
        //指定本程序的jar包所在的本地路径
        job.setJarByClass(WordcountDriver.class);

        //指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(CountMapper.class);
        job.setReducerClass(CountReducer.class);

        //指定mapper输出数据的kv类型。需要和 Mapper 中泛型的类型保持一致
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定最终输出的数据的kv类型。这里也是 Reduce 的 key，value类型。
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path("./Job1Output/part-r-00000"));
        //指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path("./Job2Output"));

        //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
        /*job.submit();*/
        boolean res = job.waitForCompletion(true);
        return res;
    }

    public static boolean job3() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        //job 是 yarn 中任务的抽象。
        Job job = Job.getInstance(conf);

        /*job.setJar("/home/hadoop/wc.jar");*/
        //指定本程序的jar包所在的本地路径
        job.setJarByClass(WordcountDriver.class);

        //指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(BuildMapper.class);
        job.setReducerClass(BuildReducer.class);

        //指定mapper输出数据的kv类型。需要和 Mapper 中泛型的类型保持一致
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //指定最终输出的数据的kv类型。这里也是 Reduce 的 key，value类型。
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path("./PROutput9/part-r-00000"));
        //指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path("./Job3Output"));

        //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
        /*job.submit();*/
        boolean res = job.waitForCompletion(true);
        return res;
    }

    public static boolean GraphBuilderJob() throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();

        //job 是 yarn 中任务的抽象。
        Job job = Job.getInstance(conf);

        /*job.setJar("/home/hadoop/wc.jar");*/
        //指定本程序的jar包所在的本地路径
        job.setJarByClass(WordcountDriver.class);

        //指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(GraphBuilder.class);
       // job.setReducerClass(GraphBuilder.Reduce.class);

        //指定mapper输出数据的kv类型。需要和 Mapper 中泛型的类型保持一致
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //指定最终输出的数据的kv类型。这里也是 Reduce 的 key，value类型。
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path("./Job3Output/part-r-00000"));
        //指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path("./GraphBuilderJobOutput"));

        //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
        /*job.submit();*/
        boolean res = job.waitForCompletion(true);
        return res;
    }

    public static boolean PageRanklterJob(int r) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();

        //job 是 yarn 中任务的抽象。
        Job job = Job.getInstance(conf);

        /*job.setJar("/home/hadoop/wc.jar");*/
        //指定本程序的jar包所在的本地路径
        job.setJarByClass(WordcountDriver.class);
        //指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(PageRanklter.PRMap.class);
        job.setReducerClass(PageRanklter.PRReduce.class);
        //指定mapper输出数据的kv类型。需要和 Mapper 中泛型的类型保持一致
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //指定最终输出的数据的kv类型。这里也是 Reduce 的 key，value类型。
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //指定job的输入原始文件所在目录
        if (r == 0)
        FileInputFormat.setInputPaths(job, new Path("./GraphBuilderJobOutput/part-r-00000"));
        else
            FileInputFormat.setInputPaths(job, new Path("./PROutput" + (r-1) + "/part-r-00000"));
        //指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path("./PROutput" + r));

        //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
        /*job.submit();*/
        boolean res = job.waitForCompletion(true);
        return res;
    }

    public static boolean PageRankViewerJob() throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();

        //job 是 yarn 中任务的抽象。
        Job job = Job.getInstance(conf);

        /*job.setJar("/home/hadoop/wc.jar");*/
        //指定本程序的jar包所在的本地路径
        job.setJarByClass(WordcountDriver.class);
        //指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(PageRankViewer.PRVMap.class);
        job.setReducerClass(PageRankViewer.PRVReduce.class);
        //指定mapper输出数据的kv类型。需要和 Mapper 中泛型的类型保持一致
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //指定最终输出的数据的kv类型。这里也是 Reduce 的 key，value类型。
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //指定job的输入原始文件所在目录

        FileInputFormat.setInputPaths(job, new Path("./PROutput9/part-r-00000"));
        //指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path("./PageRankViewerOutput"));

        //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
        /*job.submit();*/
        boolean res = job.waitForCompletion(true);
        return res;
    }

    public static boolean LPAInitJob() throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();

        //job 是 yarn 中任务的抽象。
        Job job = Job.getInstance(conf);

        /*job.setJar("/home/hadoop/wc.jar");*/
        //指定本程序的jar包所在的本地路径
        job.setJarByClass(WordcountDriver.class);
        //指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(LPAInit.LPAMap.class);
        //指定mapper输出数据的kv类型。需要和 Mapper 中泛型的类型保持一致
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //指定最终输出的数据的kv类型。这里也是 Reduce 的 key，value类型。
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //指定job的输入原始文件所在目录

        FileInputFormat.setInputPaths(job, new Path("./PROutput9/part-r-00000"));
        //指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path("./LPAInitOutput"));

        //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
        /*job.submit();*/
        boolean res = job.waitForCompletion(true);
        return res;
    }

    public static boolean LPAIterationJob(int r) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();

        //job 是 yarn 中任务的抽象。
        Job job = Job.getInstance(conf);

        /*job.setJar("/home/hadoop/wc.jar");*/
        //指定本程序的jar包所在的本地路径
        job.setJarByClass(WordcountDriver.class);
        //指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(LPAIteration.LPAMap.class);
        job.setReducerClass(LPAIteration.LPAReduce_New.class);
        //指定mapper输出数据的kv类型。需要和 Mapper 中泛型的类型保持一致
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //指定最终输出的数据的kv类型。这里也是 Reduce 的 key，value类型。
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //指定job的输入原始文件所在目录

        if (r == 0)
            FileInputFormat.setInputPaths(job, new Path("./LPAInitOutput/part-r-00000"));
        else
            FileInputFormat.setInputPaths(job, new Path("./LPAIterationOutput" + (r-1) + "/part-r-00000"));
        //指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path("./LPAIterationOutput" + r));


        //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
        /*job.submit();*/
        boolean res = job.waitForCompletion(true);
        return res;
    }

    public static boolean LPAReorganizeJob() throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();

        //job 是 yarn 中任务的抽象。
        Job job = Job.getInstance(conf);

        /*job.setJar("/home/hadoop/wc.jar");*/
        //指定本程序的jar包所在的本地路径
        job.setJarByClass(WordcountDriver.class);
        //指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(LPAReorganize.LPAMap.class);
        job.setReducerClass(LPAReorganize.LPAReduce.class);
        //指定mapper输出数据的kv类型。需要和 Mapper 中泛型的类型保持一致
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //指定最终输出的数据的kv类型。这里也是 Reduce 的 key，value类型。
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //指定job的输入原始文件所在目录

        FileInputFormat.setInputPaths(job, new Path("./LPAIterationOutput5/part-r-00000"));
        //指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path("./LPAReorganizeOutput"));

        //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
        /*job.submit();*/
        boolean res = job.waitForCompletion(true);
        return res;
    }



}
