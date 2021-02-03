package practice1;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import practice1.pojo.MarketingUserBehavior;
import practice1.pojo.UserBehavior;

import java.util.*;

/**
 * 随着智能手机的普及，在如今的电商网站中已经有越来越多的用户来自移动端，相比起传统浏览器的登录方式，手机APP成为了更多用户访问电商网站的首选。
 * 对于电商企业来说，一般会通过各种不同的渠道对自己的APP进行市场推广，而这些渠道的统计数据（比如，不同网站上广告链接的点击量、APP下载量）就成
 * 了市场营销的重要商业指标。
 *
 * APP市场推广统计 - 分渠道
 */
public class Project03_Project_AppAnalysis_By_Chanel {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                // 加载数据源
                .addSource(new AppMarketingDataSource())
                // 转换结构（(厂商, 用户行为), 次数）
                .map(behaviour -> Tuple2.of(behaviour.getChannel() + "_" + behaviour.getBehavior(), 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                // 按产商和用户行为分组
                .keyBy(t -> t.f0)
                // 求和
                .sum(1)
                .print();

        env.execute();
    }

    public static class AppMarketingDataSource extends RichSourceFunction<MarketingUserBehavior> {
        boolean canRun = true;
        Random random = new Random();

        // 模拟厂商
        List<String> channels = Arrays.asList("huawei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
        // 模拟行为
        List<String> behaviours = Arrays.asList("download", "install", "update", "uninstall");

        @Override
        public void run(SourceContext<MarketingUserBehavior> sourceContext) throws Exception {
            // 模拟无限流
            while (canRun) {
                // 随机组合，产生数据
                MarketingUserBehavior marketingUserBehavior = new MarketingUserBehavior(
                        (long) random.nextInt(1000),
                        behaviours.get(random.nextInt(behaviours.size())),
                        channels.get(random.nextInt(channels.size())),
                        System.currentTimeMillis()
                );
                sourceContext.collect(marketingUserBehavior);
                // 方便观察结果
                Thread.sleep(2000);
            }
        }

        @Override
        public void cancel() {
            canRun = false;
        }
    }
}
