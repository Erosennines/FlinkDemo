package chapter2;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Transform04_KeyBy {
    public static void main(String[] args) throws Exception {
        // keyBy：将数据分到不同的分区中，默认使用的hash分区
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // 将并行度设置为1，否则为CPU内核数

        // 需求：奇数偶数各分一组
        DataStreamSource<Integer> stream = env.fromElements(10, 3, 5, 9, 20, 8);

        // 实现一：使用匿名内部类
        KeyedStream<Integer, String> result1 = stream.keyBy(new KeySelector<Integer, String>() {
            @Override
            public String getKey(Integer integer) throws Exception {
                return integer % 2 == 0 ? "偶数" : "奇数";
            }
        });
        result1.print();

        // 实现二：使用lambda表达式
        KeyedStream<Integer, String> result = stream.keyBy(integer -> integer % 2 == 0 ? "偶数" : "奇数");
        result.print();

        // TODO 分流不是删除元素，因此打印时会将所有元素原样输出
        // TODO 有两种情况不适用于分组：
        // 1. 没有重写hashCode方法的POJO。每个元素使用Object类的hashCode方法得到的hash值都不一样，虽然代码可以运行，但无任何意义
        // 2. 任何类型的数组

        env.execute();
    }
}
