package practice1;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import practice1.pojo.AdsClickLog;

import static org.apache.flink.api.common.typeinfo.Types.*;

/**
 * 对于广告的统计，最简单也最重要的就是页面广告的点击量，网站往往需要根据广告点击量来制定定价策略和调整推广方式，
 * 而且也可以借此收集用户的偏好信息。更加具体的应用是，我们可以根据用户的地理位置进行划分，从而总结出不同省份用户
 * 对不同广告的偏好，这样更有助于广告的精准投放。
 *
 * 各省份页面广告点击量实时统计
 */
public class Project04_Ads_Click {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                // 加载数据源
                .readTextFile("in/AdClickLog.csv")
                .map(data -> {
                    String[] fields = data.split(",");
                    AdsClickLog adsClickLog = new AdsClickLog(
                            Long.parseLong(fields[0]),
                            Long.parseLong(fields[1]),
                            fields[2],
                            fields[3],
                            Long.parseLong(fields[4])
                    );

                    // ((省份，广告)，1L）
                    return Tuple2.of(Tuple2.of(adsClickLog.getProvince(), adsClickLog.getAdId()), 1L);
                })
                .returns(TUPLE(TUPLE(STRING, LONG), LONG))
                .keyBy(new KeySelector<Tuple2<Tuple2<String, Long>, Long>, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> getKey(Tuple2<Tuple2<String, Long>, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                .sum(1)
                .print("省份-广告：");

        env.execute();
    }
}
