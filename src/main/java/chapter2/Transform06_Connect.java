package chapter2;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Transform06_Connect {
    public static void main(String[] args) throws Exception {
        // connect：将两条流进行连接
        // 1. 两条流的类型可以不同
        // 2. 只是机械的合并，内部仍然是分离的两条流
        // 3. 只能有两条流参与，不能有第三条流
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // 将并行度设置为1，否则为CPU内核数

        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<String> stream2 = env.fromElements("a", "b", "c");

        ConnectedStreams<Integer, String> result = stream1.connect(stream2);
        result.getFirstInput().print("first stream: ");
        result.getSecondInput().print("second stream: ");

        env.execute();
    }
}
