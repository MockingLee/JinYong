package neu;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.NlpAnalysis;

import java.util.List;

public class Test2 {

    public static void main(String[] args) {
// 增加新词,中间按照'\t'隔开
        String s = "我是:我我大多数";
        System.out.println(s.split(":")[0]);
        System.out.println(s.split(":")[1]);

    }

}
