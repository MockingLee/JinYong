package neu;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class LPAIteration {

    public static class LPAMap extends Mapper<LongWritable , Text, Text , Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("\t");
            Text name = new Text(strs[0].split("#")[1]);
            for (String s : strs[1].split("#")[1].split(";")){
                context.write(new Text(s.split(":")[0]) , new Text(strs[0].split("#")[0] + "#" + name));
            }

            context.write(name , new Text("!" + strs[1].split("#")[1]));//<人物名，关系链表>
            context.write(name , new Text("@" + strs[0].split("#")[2]));//<人物名，PageRank值>
            context.write(name , new Text("$" + strs[0].split("#")[0]));//<人物名，标签>

        }
    }

    public static class LPAReduce extends Reducer<Text , Text , Text , Text>{
        static HashMap<String , String> map = new HashMap<>();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> linkStart = new ArrayList<>();
            String links = null;
            String pr = null;
            String label = null;
            ArrayList<String> max_list = new ArrayList<>();
            Map<String,String> relation_name_label = new HashMap<>();
            for (Text t : values){
                if (t.toString().startsWith("!")){
                    links = t.toString();
                    String[] strs = links.toString().split("!")[1].split(";");
                    for (String s : strs){
                        if (max_list.isEmpty())
                            max_list.add(s);
                        else {
                            float cur_f = Float.parseFloat(s.split(":")[1]);
                            if (cur_f >= Float.parseFloat(max_list.get(max_list.size() - 1).split(":")[1])){
                                max_list.add(s);
                            }
                        }
                    }
                }

                else if (t.toString().startsWith("@"))
                    pr = t.toString();
                else if (t.toString().startsWith("$"))
                    label = t.toString();
                else {
                    linkStart.add(t.toString());
                    relation_name_label.put(t.toString().split("#")[1] , t.toString().split("#")[0]);
                }
            }


            Map<String,Float> label_pr_map = new HashMap<>();
            StringTokenizer nameList_Tokenizer = new StringTokenizer(links.replace("!" , ""),";");
            while(nameList_Tokenizer.hasMoreTokens()){
                String[] name_pr = nameList_Tokenizer.nextToken().split(":");
                Float current_pr = Float.parseFloat(name_pr[1]);
                String current_label = relation_name_label.get(name_pr[0]);
                Float label_pr;
                if ((label_pr = label_pr_map.get(current_label)) != null){
                    label_pr_map.put(current_label,label_pr+current_pr);
                }else{
                    label_pr_map.put(current_label,current_pr);
                }
            }


            StringTokenizer tokenizer = new StringTokenizer(links.replace("!" , ""),";");
            float maxPr = Float.MIN_VALUE;
            List<String> maxNameList = new ArrayList<>();
            while (tokenizer.hasMoreTokens()){
                String[] element = tokenizer.nextToken().split(":");
                float tmpPr = label_pr_map.get(relation_name_label.get(element[0]));
                if (maxPr < tmpPr){
                    maxNameList.clear();
                    maxPr = tmpPr;
                    maxNameList.add(element[0]);
                }else if (maxPr == tmpPr){
                    maxNameList.add(element[0]);
                }
            }

            Random random = new Random();
            int index = random.nextInt(maxNameList.size());
            String target_name = maxNameList.get(index);
            String target_label = relation_name_label.get(target_name);
            if (map.get(target_name) != null){
                target_label = map.get(target_name);
            }else{
                map.put(key.toString(),target_label);
            }
            if (target_label == null){
                System.out.println();
            }

            context.write(new Text(target_label + "#" + key.toString() + "#" + pr.toString().split("@")[1]) , new Text(pr.toString().split("@")[1] + "#" + links.toString().split("!")[1]));

//            int index = max_list.size() - 2;
//            float pre = Float.parseFloat(max_list.get(max_list.size() - 1).split(":")[1]);
//            List<String> max = new ArrayList<>();
//            max.add(max_list.get(max_list.size() - 1));
//            while(index >= 0){
//                if (Float.parseFloat(max_list.get(index).split(":")[1]) != pre)
//                    break;
//                else {
//                    max.add(max_list.get(index));
//
//                    pre = Float.parseFloat(max_list.get(index).split(":")[1]);
//                    index -- ;
//                }
//            }

//            Random random = new Random();
//            int max_index = random.nextInt(max.size());
//            String j = max.get(max_index);
//            String label_new = label.toString();
//            for (String s : linkStart){
//                if (j.split(":")[0].equals(s.split("#")[1]))
//                    if (map.containsKey(j.split(":")[0]))
//                        label_new = map.get(j.split(":")[0]);
//                    else {
//                        label_new = s.split("#")[0];
//                        map.put(j.split(":")[0] , label_new);
//                    }
//            }
//            String key_ = label_new + "#" + key.toString() + "#" + pr.toString().split("@")[1];
//            String value_ = pr.toString().split("@")[1] + "#" + links.toString().split("!")[1];
//            context.write(new Text(label_new + "#" + key + "#" + pr.toString().split("@")[1]) , new Text(pr.toString().split("@")[1] + "#" + links.toString().split("!")[1]));


        }
    }

    public static class LPAReduce_New extends Reducer<Text , Text , Text , Text>{
        Map<String,String> name_label_map = new HashMap<>();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String label = "";
            String nameList = "";
            String pr = "";
            Map<String,String> relation_name_label = new HashMap<>();
            for(Text text:values){
                String str = text.toString();
                if (str.length() > 0 && str.charAt(0) == '$'){
                    label = str.replace("$","");
                }else if (str.length() > 0 &&str.charAt(0) == '@'){
                    pr = str.replace("@","");
                }else if (str.length() > 0 &&str.charAt(0) == '!'){
                    nameList = str.replace("!","");
                }else if (str.length() > 0){
                    String[] element = str.split("#");
                    relation_name_label.put(element[1],element[0]);
                }
            }

            Map<String,Float> label_pr_map = new HashMap<>();
            StringTokenizer nameList_Tokenizer = new StringTokenizer(nameList,";");
            while(nameList_Tokenizer.hasMoreTokens()){
                String[] name_pr = nameList_Tokenizer.nextToken().split(":");
                Float current_pr = Float.parseFloat(name_pr[1]);
                String current_label = relation_name_label.get(name_pr[0]);
                Float label_pr;
                if ((label_pr = label_pr_map.get(current_label)) != null){
                    label_pr_map.put(current_label,label_pr+current_pr);
                }else{
                    label_pr_map.put(current_label,current_pr);
                }
            }


            StringTokenizer tokenizer = new StringTokenizer(nameList,";");
            float maxPr = Float.MIN_VALUE;
            List<String> maxNameList = new ArrayList<>();
            while (tokenizer.hasMoreTokens()){
                String[] element = tokenizer.nextToken().split(":");
                float tmpPr = label_pr_map.get(relation_name_label.get(element[0]));
                if (maxPr < tmpPr){
                    maxNameList.clear();
                    maxPr = tmpPr;
                    maxNameList.add(element[0]);
                }else if (maxPr == tmpPr){
                    maxNameList.add(element[0]);
                }
            }

            Random random = new Random();
            int index = random.nextInt(maxNameList.size());
            String target_name = maxNameList.get(index);
            String target_label = relation_name_label.get(target_name);
            if (name_label_map.get(target_name) != null){
                target_label = name_label_map.get(target_name);
            }else{
                name_label_map.put(key.toString(),target_label);
            }
            if (target_label == null){
                System.out.println();
            }
            context.write(new Text(target_label + "#" + key.toString() + "#" +  pr),new Text(pr + "#" + nameList));
        }
    }

}
