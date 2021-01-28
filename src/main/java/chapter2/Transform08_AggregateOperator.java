package chapter2;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import pojo.WaterSensor;

public class Transform08_AggregateOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 常见的聚合算子：sum, min. max, minBy, maxBy
        // keyedStream的每一条支流做聚合。执行完成后，会将聚合的结果合成一条流返回，结果都是DataStream

        // 示例1：流中存储的是元组
        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3, 4, 5);
        KeyedStream<Integer, String> keyedStream1 = stream1.keyBy(ele -> ele % 2 == 0 ? "奇数" : "偶数");

        keyedStream1.sum(0).print(); // 最终两条流的结果为：6，9
        keyedStream1.max(0).print(); // 4，5
        keyedStream1.min(0).print(); // 1，2

        // 当流中存储的是元组时，参数就是位置（基于0）；示例1中只有一个参数，因此聚合函数的参数只能为0，否则会报错

        // 示例2：流中存储的时POJO类（或scala的样例类）
        DataStreamSource<WaterSensor> stream2 = env.fromElements(
                new WaterSensor("sensor_1", 1607527994000L, 50),
                new WaterSensor("sensor_1", 1607527996000L, 30),
                new WaterSensor("sensor_2", 1607527993000L, 10),
                new WaterSensor("sensor_2", 1607527995000L, 30)
        );

        KeyedStream<WaterSensor, String> keyedStream2 = stream2.keyBy(WaterSensor::getId);
        keyedStream2.sum("vc").print();

        // 当流中存储的时POJO类时，参数就是字段名

        env.execute("Transform08_AggregateOperator");
    }
}
