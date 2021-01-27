package chapter2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Transform02_FlatMap {
    public static void main(String[] args) throws Exception {
        // flatMap：消费一个元素返回零个或多个元素
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // 将并行度设置为1，否则为CPU内核数
        DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 4, 5);

        // 需求：求流中每个元素的平方值和立方值
        // 实现一：使用匿名内部类
        SingleOutputStreamOperator<Integer> result1 = stream.flatMap(new FlatMapFunction<Integer, Integer>() {
            @Override
            public void flatMap(Integer integer, Collector<Integer> collector) throws Exception {
                collector.collect(integer * integer);
                collector.collect(integer * integer * integer);
            }
        });

        result1.print();

        // 实现二：使用lambda表达式
        SingleOutputStreamOperator<Integer> result2 = stream
                .flatMap((Integer integer, Collector<Integer> collector) -> {
                    collector.collect(integer * integer);
                    collector.collect(integer * integer * integer);
                })
                .returns(Types.INT);    // 类型擦除，需要明确返回值类型
        // 什么是类型擦除：https://www.cnblogs.com/wuqinglong/p/9456193.html

        result2.print();

        env.execute();
    }
}
