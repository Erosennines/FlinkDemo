package practice1;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import pojo.WaterSensor;
import practice1.pojo.UserBehavior;

import java.util.HashSet;
import java.util.Set;

/**
 * 上一个案例中，我们统计的是所有用户对页面的所有浏览行为，也就是说，同一用户的浏览行为会被重复统计。
 * 而在实际应用中，我们往往还会关注，到底有多少不同的用户访问了网站，所以另外一个统计流量的重要指标是网站的独立访客数（Unique Visitor，UV）
 * 需要注意的是，PV包含UV
 */
public class Project02_UV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 加载数据源
        DataStreamSource<String> stream = env.readTextFile("in/UserBehavior.csv");

        stream
                // 将数据转换结构
                .flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] fields = line.split(",");
                    UserBehavior userBehavior = new UserBehavior(
                            Long.parseLong(fields[0]),
                            Long.parseLong(fields[1]),
                            Integer.parseInt(fields[2]),
                            fields[3],
                            Long.parseLong(fields[4])
                    );
                    // 先过滤出pv行为，减少数据量，提升效率
                    if ("pv".equals(userBehavior.getBehavior())) {
                        out.collect(Tuple2.of("uv", userBehavior.getUserId()));
                    }
                })
                // 类型擦除，需要显式声明类型
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                // 分组
                .keyBy(behavior -> behavior.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, Integer>() {
                    // 利用HashSet去重
                    Set<Long> userId = new HashSet<>();
                    @Override
                    public void processElement(Tuple2<String, Long> value, Context context, Collector<Integer> collector) throws Exception {
                        userId.add(value.f1);
                        // hashset的大小就是UV
                        collector.collect(userId.size());
                    }
                })
                .print();

        env.execute();
    }
}
