package practice1;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import practice1.pojo.OrderEvent;
import practice1.pojo.TxEvent;

import java.util.HashMap;
import java.util.Map;

/**
 * 在电商网站中，订单的支付作为直接与营销收入挂钩的一环，在业务流程中非常重要。对于订单而言，为了正确控制业务流程，
 * 也为了增加用户的支付意愿，网站一般会设置一个支付失效时间，超过一段时间不支付的订单就会被取消。另外，对于订单的支付，
 * 我们还应保证用户支付的正确性，这可以通过第三方支付平台的交易数据来做一个实时对账。
 *
 * TODO 需求: 来自两条流的订单交易匹配
 *
 * 对于订单支付事件，用户支付完成其实并不算完，我们还得确认平台账户上是否到账了。而往往这会来自不同的日志信息，所以我们要同时读入两条流的数据来做合并处理。
 */
public class Project05_Order {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        SingleOutputStreamOperator<OrderEvent> orderEventDS = env
                // 1. 加载数据源。订单数据从OrderLog.csv中读取
                .readTextFile("in/OrderLog.csv")
                .map(data -> {
                    String[] fields = data.split(",");
                    return new OrderEvent(
                            Long.valueOf(fields[0]),
                            fields[1],
                            fields[2],
                            Long.parseLong(fields[3])
                    );
                });

        SingleOutputStreamOperator<TxEvent> txDS = env
                // 2. 加载数据源。交易数据从ReceiptLog.csv中读取
                .readTextFile("in/ReceiptLog.csv")
                .map(data -> {
                    String[] fields = data.split(",");
                    return new TxEvent(
                            fields[0],
                            fields[1],
                            Long.valueOf(fields[2])
                    );
                });

        // 3. 双流合并
        ConnectedStreams<OrderEvent, TxEvent> orderAndTx = orderEventDS.connect(txDS);

        // 4. 两条流中的数据到达的时间无法确定，有快有慢，所以要匹配对账信息
        orderAndTx
                // keyBy要有两个参数，针对两条流。别忘记connect虽然是合并流，但是它内部仍然是两条独立的流
                .keyBy("txId", "txId")
                .process(new CoProcessFunction<OrderEvent, TxEvent, String>() {
                    // 存储：txId -> OrderEvent
                    Map<String, OrderEvent> orderMap = new HashMap<>();
                    // 存储：txId -> TxEvent
                    Map<String, TxEvent> txMap = new HashMap<>();

                    @Override
                    public void processElement1(OrderEvent orderEvent, Context context, Collector<String> collector) {
                        // 获取交易信息
                        // 如果第一条流中包含第二条流的txId，说明下单并且支付了；否则要把数据假如到第二条流中
                        if (txMap.containsKey(orderEvent.getTxId())) {
                            collector.collect("订单：" + orderEvent + " 对账成功");
                            // 对账完成之后删除，避免重复
                            txMap.remove(orderEvent.getTxId());
                        } else {
                            orderMap.put(orderEvent.getTxId(), orderEvent);
                        }
                    }

                    @Override
                    public void processElement2(TxEvent txEvent, Context context, Collector<String> collector) {
                        // 获取订单信息
                        // 由于流数据的不确定性，在实时获取数据时系统可能先收到支付的信息，在收到下单的信息，因此需要对第二条流做相同处理
                        if (orderMap.containsKey(txEvent.getTxId())) {
                            OrderEvent orderEvent = orderMap.get(txEvent.getTxId());
                            collector.collect("订单：" + orderEvent + "对账成功");
                            orderMap.remove(txEvent.getTxId());
                        } else {
                            txMap.put(txEvent.getTxId(), txEvent);
                        }
                    }
                })
                .print();

        env.execute();
    }
}
