package chapter1;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import pojo.WaterSensor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class Flink05_Source_Customize {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 使用自定义数据源
        DataStreamSource<WaterSensor> stream = env.addSource(new MySource("localhost", 9999));
        stream.print();

        env.execute("Flink05_Source_Customize");
    }

    public static class MySource implements SourceFunction<WaterSensor> {
        private String host;
        private int port;
        private volatile boolean isRunning = true;
        private Socket socket;

        public MySource(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public void run(SourceContext<WaterSensor> sourceContext) throws Exception {
            // 实现从一个socket中读取数据
            socket = new Socket(host, port);
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

            String line;
            while (isRunning && ((line = reader.readLine()) != null)) {
                String[] split = line.split(",");
                sourceContext.collect(new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2])));
            }
        }

        @Override
        public void cancel() {
            isRunning = false;

            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
