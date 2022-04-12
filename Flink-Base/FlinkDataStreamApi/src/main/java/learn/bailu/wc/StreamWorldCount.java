package learn.bailu.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.lang.reflect.Parameter;

public class StreamWorldCount {
    public static void main(String[] args) throws Exception {
        // 1. Create a stream excute environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Read hostname and ip from parameters
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("hostname", "localhost");
        Integer port = parameterTool.getInt("port", 9999);

        // 2. Read text stream from socket
        DataStreamSource<String> dataStreamSource = env.socketTextStream(hostname, port);

        // 3. convert compute
        SingleOutputStreamOperator<Tuple2<String, Long>> worldTuple = (SingleOutputStreamOperator<Tuple2<String, Long>>) dataStreamSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(new Tuple2<>(word, 1L));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4. grorp by key
        KeyedStream<Tuple2<String, Long>, String> worldAndOneKeydStream = worldTuple.keyBy(data -> data.f0);

        // 5. sum
        SingleOutputStreamOperator<Tuple2<String, Long>> worldAndSum = worldAndOneKeydStream.sum(1);

        // 6. print
        worldAndSum.print();

        // 7. execute
        env.execute();
    }
}
