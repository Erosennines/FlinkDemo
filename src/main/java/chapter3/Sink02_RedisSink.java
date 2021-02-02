package chapter3;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import pojo.WaterSensor;

import java.util.ArrayList;
import java.util.List;

public class Sink02_RedisSink {
    // redis sink
    public static void main(String[] args) throws Exception {
        // 首先需要添加redis Connector依赖
        List<WaterSensor> waterSensors = new ArrayList<>();

        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> stream = env.fromCollection(waterSensors);

        // 将流中的内容写入redis

        // 首先创建redis的连接器
        FlinkJedisPoolConfig redisConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop122")
                .setPort(6379)
                .setMaxTotal(100)
                .setTimeout(1000 * 10)
                .build();

        stream.addSink(new RedisSink<>(redisConfig, new RedisMapper<WaterSensor>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                // 指定写入redis的数据类型
                return new RedisCommandDescription(RedisCommand.HSET, "sensor");
            }

            @Override
            public String getKeyFromData(WaterSensor waterSensors) {
                // 指定key
                return waterSensors.getId();
            }

            @Override
            public String getValueFromData(WaterSensor waterSensors) {
                // 指定value
                return JSON.toJSONString(waterSensors);
            }
        }));

        env.execute();
    }
}
