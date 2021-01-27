package chapter2;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 第二节主要是各种算子的使用
public class Transform01_Map {
    public static void main(String[] args) throws Exception {
        // map算子：将数据流中的数据进行转换，形成新的数据流，消费一个元素并产出一个元素
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 案例：实现对流中的每个元素平方
        // 实现一：使用匿名内部类的方式实现需求
        DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 4, 5);

        SingleOutputStreamOperator<Integer> result1 = stream.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer integer) throws Exception {
                return integer * integer;
            }
        });
        result1.print();

        // 实现2：使用Java8 lambda表达式实现
        SingleOutputStreamOperator<Integer> result2 = stream.map(ele -> ele * ele);
        result2.print();

        // 实现3：使用静态内部类实现
        SingleOutputStreamOperator<Integer> result3 = stream.map(new SquareFunction());
        result3.print();

        env.execute("Transform01_Map");
    }

    public static class SquareFunction implements MapFunction<Integer, Integer> {
        @Override
        public Integer map(Integer integer) throws Exception {
            return integer * integer;
        }
    }
}
