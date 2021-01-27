package chapter2;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Transform03_Filter {
    public static void main(String[] args) throws Exception {
        // filter：保留符合条件的数据
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // 将并行度设置为1，否则为CPU内核数

        // 需求：保留偶数
        DataStreamSource<Integer> stream = env.fromElements(10, 3, 5, 9, 20, 8);

        // 实现一：使用匿名内部类
        SingleOutputStreamOperator<Integer> result1 = stream
                .filter(new FilterFunction<Integer>() {
                    @Override
                    public boolean filter(Integer integer) throws Exception {
                        return integer % 2 == 0;
                    }
                });

        result1.print();

        // 实现二：使用lambda表达式
        SingleOutputStreamOperator<Integer> result2 = stream
                .filter(integer -> integer % 2 == 0);

        result2.print();

        env.execute();
    }
}
