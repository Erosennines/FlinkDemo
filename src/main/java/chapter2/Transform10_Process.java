package chapter2;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import pojo.WaterSensor;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;

public class Transform10_Process {
    public static void main(String[] args) throws Exception {
        // process算子是一个比较底层的算子，它可以获取很多流中的信息
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1607527992000L, 20),
                new WaterSensor("sensor_1", 1607527996000L, 50),
                new WaterSensor("sensor_1", 1607527996000L, 50),
                new WaterSensor("sensor_2", 1607527993000L, 10),
                new WaterSensor("sensor_2", 1607527995000L, 30)

        );

        // 示例一：在keyBy之前使用
        SingleOutputStreamOperator<Tuple2<String, Integer>> result1 = stream.process(new ProcessFunction<WaterSensor, Tuple2<String, Integer>>() {
            @Override
            public void processElement(WaterSensor waterSensors, Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // 获取流中的元素
                collector.collect(new Tuple2<>(waterSensors.getId(), waterSensors.getVc()));
            }
        });

        result1.print();

        // 示例二：在keyBy之后使用
        stream
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, Tuple2<String, Integer>>() {
                    @Override
                    public void processElement(WaterSensor waterSensor, Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        // 可以通过上下文对象获取当前对应的key，还可以获取对应的窗口信息(后续详细说明)
                        collector.collect(new Tuple2<>("key是：" + context.getCurrentKey(), waterSensor.getVc()));
                    }
                })
                .print();

        env.execute();
    }
}
