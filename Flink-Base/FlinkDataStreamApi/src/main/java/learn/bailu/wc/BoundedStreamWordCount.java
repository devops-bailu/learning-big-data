package learn.bailu.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class BoundedStreamWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 从文件中读取数据
        DataStreamSource<String> lineDataStreamSource = env.readTextFile("input/words.txt");

        // 3. 转换计算
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple = lineDataStreamSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        })
                // 使用了 lamdba 表达式，有范型擦除，所以需要声明类型
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4. 分组
        KeyedStream<Tuple2<String, Long>, String> worlAndOneKeyedStream = wordAndOneTuple.keyBy(data -> data.f0);

        // 5. 求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = worlAndOneKeyedStream.sum(1);

        // 6. 打印
        sum.print();

        // 7. 启动执行
        env.execute();
    }
}
