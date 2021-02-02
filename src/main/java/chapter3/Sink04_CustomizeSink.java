package chapter3;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import pojo.WaterSensor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

/**
 * 如果Flink没有提供给我们可以直接使用的连接器，这时需要我们自定义
 *
 * 自定义MySQL Sink
 */
public class Sink04_CustomizeSink {
    // customize mysql sink
    // 1. 在MySQL中建表
    // create database test;
    // use test;
    // CREATE TABLE `sensor` (
    //   `id` varchar(20) NOT NULL,
    //   `ts` bigint(20) NOT NULL,
    //   `vc` int(11) NOT NULL,
    //   PRIMARY KEY (`id`,`ts`)
    // ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    public static void main(String[] args) throws Exception {
        // 2. 导入MySQL依赖
        // 3. 编写代码
        List<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> stream = env.fromCollection(waterSensors);

        // RichSinkFunction: 自定义sink
        stream.addSink(new RichSinkFunction<WaterSensor>() {
            private PreparedStatement ps;
            private Connection conn;

            // open方法相当于初始化，只会执行一次：配置MySQL连接信息
            public void open(Configuration parameters) throws Exception {
                conn = DriverManager.getConnection("jdbc:mysql://hadoop122:3306/test?useSSL=false", "root", "000000");
                ps = conn.prepareStatement("insert into sensor values (?, ?, ?)");
            }

            // 同open方法
            public void close() throws Exception {
                ps.close();
                conn.close();
            }

            // 方法执行体
            public void invoke(WaterSensor value, Context context) throws Exception {
                ps.setString(1, value.getId());
                ps.setLong(2, value.getTs() );
                ps.setInt(3, value.getVc());
                ps.execute();
            }
        });

        env.execute();
    }
}
