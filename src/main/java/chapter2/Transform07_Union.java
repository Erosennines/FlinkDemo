package chapter2;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Transform07_Union {
    public static void main(String[] args) throws Exception {
        // union：将两条流或多条流进行连接
        // 1. 流的类型必须一致
        // 2. 可以连接多条流
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // 将并行度设置为1，否则为CPU内核数

        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Integer> stream2 = env.fromElements(10, 20, 30, 40, 50);
        DataStreamSource<Integer> stream3 = env.fromElements(100, 200, 300, 400, 500);

        DataStream<Integer> result = stream1.union(stream2).union(stream3);
        result.print();

        env.execute();
    }
}
