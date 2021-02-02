package practice1;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import practice1.pojo.UserBehavior;

/**
 * 衡量网站流量一个最简单的指标，就是网站的页面浏览量（Page View，PV）。用户每次打开一个页面便记录1次PV，多次打开同一页面则浏览量累计。
 * 一般来说，PV与来访者的数量成正比，但是PV并不直接决定页面的真实来访者数量，如同一个来访者通过不断的刷新页面，也可以制造出非常高的PV。
 */
public class Project01_PV_1 {
    // 实现一：使用算子实现（wordcount思想）
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 加载数据源
        DataStreamSource<String> stream = env.readTextFile("in/UserBehavior.csv");

        stream
                // 将数据转换结构
                .map(line -> {
                    String[] fields = line.split(",");
                    return new UserBehavior(
                            Long.parseLong(fields[0]),
                            Long.parseLong(fields[1]),
                            Integer.parseInt(fields[2]),
                            fields[3],
                            Long.parseLong(fields[4])
                    );
                })
                // 先过滤出pv行为，减少数据量，提升效率
                .filter(r -> "pv".equals(r.getBehavior()))
                .map(behavior -> Tuple2.of("pv", 1L))
                // 类型擦除，需要显式说明类型
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(behavior -> behavior.f0)
                // 求和
                .sum(1)
                .print();

        env.execute();
    }
}
