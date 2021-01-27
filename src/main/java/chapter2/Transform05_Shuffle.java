package chapter2;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Transform05_Shuffle {
    public static void main(String[] args) throws Exception {
        // shuffle：把流中的元素随机打乱
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // 将并行度设置为1，否则为CPU内核数

        // 多流交汇时，流中数据发生重组
        // 具体作用需要在具体案例中体会
        DataStreamSource<Integer> stream = env.fromElements(10, 3, 5, 9, 20, 8);

        DataStream<Integer> result = stream.shuffle();
        result.print();

        env.execute();
    }
}
