package chapter2;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import pojo.WaterSensor;
import scala.Tuple2;

// 这里介绍几种对流重新分区的算子
public class Transform11_RePartition {
    public static void main(String[] args) throws Exception {
        // keyBy算子：先按照key分组，按照key的双重分区来选择后面的分区
        // shuffle算子：对流中的元素随即分区
        // rebalance：对流中的元素平均分布到每个区。当处理倾斜数据的时候, 进行性能优化
        // rescale：和rebalance一样, 也是平均循环的分布数据. 但是要比rebalance更高效，因为rescale不需要通过网络，完全走的"管道"
    }
}
