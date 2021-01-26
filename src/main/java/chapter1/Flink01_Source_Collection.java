package chapter1;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import pojo.WaterSensor;

import java.util.Arrays;
import java.util.List;

// 从集合中读取数据
public class Flink01_Source_Collection {
    public static void main(String[] args) throws Exception {
        List<WaterSensor> waterSensors = Arrays.asList(
                new WaterSensor("ws_001", 1577844001L, 45),
                new WaterSensor("ws_002", 1577844015L, 43),
                new WaterSensor("ws_003", 1577844020L, 42)
        );

        // 从Java集合中读取数据
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromCollection(waterSensors).print();

        env.execute("Flink01_Source_Collection");
    }
}
