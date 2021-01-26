package chapter1;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink03_Source_UnboundedFlow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从无限流中读取数据（若在windows下执行，需要安装netcat工具，在安装目录下cmd输入：nc -lp 9999）
        DataStreamSource<String> stream = env.socketTextStream("localhost", 9999);
        stream.print();

        env.execute("Flink03_Source_UnboundedFlow");
    }
}
