package neu;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.NlpAnalysis;

import java.util.List;

public class Test2 {

    public static void main(String[] args) {
// 增加新词,中间按照'\t'隔开
        String str = "这是一段测试文字";
        //DicLibrary.insert(DicLibrary.DEFAULT, "这是");//设置自定义分词
        Result result= NlpAnalysis.parse(str);
        List<Term> termList=result.getTerms();
        for(Term term:termList){
            System.out.println(term.getName()+":"+term.getNatureStr());
        }

    }

}
