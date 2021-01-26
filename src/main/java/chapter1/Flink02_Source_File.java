package chapter1;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink02_Source_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据
        DataStreamSource<String> stream = env.readTextFile("in/C1_02.txt");
        stream.print();

        env.execute("Flink02_Source_File");
    }
}
