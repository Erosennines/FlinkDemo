package chapter3;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import pojo.WaterSensor;

import java.util.ArrayList;
import java.util.List;

/**
 * 这一节主要介绍常用的sink算子
 * 所谓sink算子就是将处理完的数据写入存储系统的输出操作
 */
public class Sink01_KafkaSink {
    // kafka sink
    public static void main(String[] args) throws Exception {
        // 首先需要添加Kafka Connector依赖
        List<WaterSensor> waterSensors = new ArrayList<>();

        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<List<WaterSensor>> stream = env.fromElements(waterSensors);

        // 将流中的内容转成json字符串写入kafka
        SingleOutputStreamOperator<String> mapTrans = stream.map(JSON::toJSONString);

        // 指定kafka主机+端口，主题名，序列化方式
        mapTrans.addSink(new FlinkKafkaProducer<String>("hadoop122:9092", "topic_sensor", new SimpleStringSchema()));

        env.execute();
    }
}
