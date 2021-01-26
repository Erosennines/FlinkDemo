package chapter1;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class Flink04_Source_Kafka {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        /*
           put和setProperty的区别：
           因为 Properties 继承于 Hashtable，所以可对 Properties 对象应用 put 和 putAll 方法。
           但强烈反对使用这两个方法，因为它们允许调用方插入其键或值不是 Strings 的项。
           相反，应该使用 setProperty 方法。
           如果在“有危险”的 Properties 对象（即包含非 String 的键或值）上调用 store 或 save 方法，则该调用将失败。
         */
        properties.setProperty("bootstrap.servers", "hadoop122:9092,hadoop123:9092,hadoop124:9092");
        properties.setProperty("group.id", "Flink04_Source_Kafka");
        properties.setProperty("auto.offset.reset", "latest");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer<>("sensor", new SimpleStringSchema(), properties));
        stream.print("kafka source");

        env.execute("Flink04_Source_Kafka");
    }
}
