package chapter3;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;
import pojo.WaterSensor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Sink03_ESSink {
    // es sink
    public static void main(String[] args) throws Exception {
        // 首先需要添加elasticsearch Connector依赖
        List<WaterSensor> waterSensors = new ArrayList<>();

        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

        // ES集群信息
        List<HttpHost> esHosts = Arrays.asList(
                new HttpHost("hadoop122", 9200),
                new HttpHost("hadoop123", 9200),
                new HttpHost("hadoop124", 9200)
        );

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> stream = env.fromCollection(waterSensors);

        // 将流中的内容写入es
        stream.addSink(new ElasticsearchSink.Builder<WaterSensor>(esHosts, new ElasticsearchSinkFunction<WaterSensor>() {
            @Override
            public void process(WaterSensor waterSensor, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                IndexRequest request = Requests
                        .indexRequest("sensor")
                        .type("_doc")
                        .id(waterSensor.getId())
                        .source(JSON.toJSONString(waterSensor), XContentType.JSON);
                requestIndexer.add(request);
            }
        }).build());

        env.execute();
    }
}
