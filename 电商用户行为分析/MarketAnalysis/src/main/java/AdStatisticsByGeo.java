import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Description: java类作用描述
 * @Author: Xiaoyuyu
 * @CreateDate: 2021/7/25 8:24 下午
 */

// 定义输入数据样例类
// case class AdClickEvent(userId:Long,adId:Long,province:String,city:String,timestamp:Long)
class AdClickEvent {
    public Long userId;
    public Long adId;
    public String province;
    public String city;
    public Long timestamp;

    public AdClickEvent(Long userId, Long adId, String province, String city, Long timestamp) {
        this.userId = userId;
        this.adId = adId;
        this.province = province;
        this.city = city;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "AdClickEvent{" +
                "userId=" + userId +
                ", adId=" + adId +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
// 定义输出数据样例类
// case class AdCountByProvince(province:String,windowEnd:String,count:Long)
class AdCountByProvince {
    public String province;
    public String windowEnd;
    public Long count;

    public AdCountByProvince(String province, String windowEnd, Long count) {
        this.province = province;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    @Override
    public String toString() {
        return "AdCountByProvince{" +
                "province='" + province + '\'' +
                ", windowEnd='" + windowEnd + '\'' +
                ", count=" + count +
                '}';
    }
}

public class AdStatisticsByGeo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStreamSource<String> stringDataStreamSource = env.readTextFile("data/AdClickLog.csv");

        stringDataStreamSource.map(new MapFunction<String, AdClickEvent>() {
            @Override
            public AdClickEvent map(String s) throws Exception {
                String[] s1 = s.split(",");
                return new AdClickEvent(Long.parseLong(s1[0]),Long.parseLong(s1[1]),s1[2],s1[3],Long.parseLong(s1[4]));
            }
        })
        .assignTimestampsAndWatermarks(WatermarkStrategy.<AdClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
        .withTimestampAssigner((event,timestamp)->event.timestamp*1000))
        .keyBy(value->value.province)
        .timeWindow(Time.minutes(60),Time.seconds(5))
        .process(new AdCount())
        .print();

        env.execute();
    }
}

class AdCount extends ProcessWindowFunction<AdClickEvent, String, String, TimeWindow> {

    @Override
    public void process(String s, Context context, Iterable<AdClickEvent> elements, Collector<String> out) throws Exception {
        String windowEnd = String.valueOf(context.window().getEnd());
        long count = 0L;
        for(AdClickEvent ignored :elements) {
            count++;
        }
        AdCountByProvince ans = new AdCountByProvince(s, windowEnd, count);
        out.collect(ans.toString());
    }
}
