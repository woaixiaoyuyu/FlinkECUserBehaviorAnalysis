import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Random;


/**
 * @Description: java类作用描述
 * @Author: Xiaoyuyu
 * @CreateDate: 2021/7/25 3:47 下午
 */

// 定义一个输入数据的样例类  保存电商用户行为的样例类
// case class MarketingUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)
class MarketingUserBehavior {
    public String userId;
    public String behavior;
    public String channel;
    public Long timestamp;

    public MarketingUserBehavior(String userId, String behavior, String channel, Long timestamp) {
        this.userId = userId;
        this.behavior = behavior;
        this.channel = channel;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "MarketingUserBehavior{" +
                "userId='" + userId + '\'' +
                ", behavior='" + behavior + '\'' +
                ", channel='" + channel + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
// 定义一个输出结果的样例类   保存 市场用户点击次数
// case class MarketingViewCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)
class MarketingViewCount {
    public String windowStart;
    public String windowEnd;
    public String channel;
    public String behavior;
    public Long count;

    public MarketingViewCount(String windowStart, String windowEnd, String channel, String behavior, Long count) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.channel = channel;
        this.behavior = behavior;
        this.count = count;
    }

    @Override
    public String toString() {
        return "MarketingViewCount{" +
                "windowStart='" + windowStart + '\'' +
                ", windowEnd='" + windowEnd + '\'' +
                ", channel='" + channel + '\'' +
                ", behavior='" + behavior + '\'' +
                ", count=" + count +
                '}';
    }
}

public class AppMarketingByChannel {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 自定义数据源
        DataStreamSource<MarketingUserBehavior> marketingUserBehaviorDataStreamSource = env.addSource(new SimulateEventSource());

        marketingUserBehaviorDataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<MarketingUserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5))
        .withTimestampAssigner((event,timeStamp)->event.timestamp))
        .filter(value-> !value.behavior.equals("UNINSTALL"))
        .map(new MapFunction<MarketingUserBehavior, Tuple2<Tuple2<String,String>,Long>>() {
            @Override
            public Tuple2<Tuple2<String,String>, Long> map(MarketingUserBehavior marketingUserBehavior) throws Exception {
                return new Tuple2<>(new Tuple2<>(marketingUserBehavior.channel, marketingUserBehavior.behavior), 1L);
            }
        })
        .keyBy(new KeySelector<Tuple2<Tuple2<String, String>, Long>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Tuple2<Tuple2<String, String>, Long> tuple2LongTuple2) throws Exception {
                return tuple2LongTuple2.f0;
            }
        })
        .timeWindow(Time.minutes(60),Time.seconds(10))
        .process(new MarketingCountByChannel())
        .print();

        env.execute();
    }
}

class SimulateEventSource extends RichParallelSourceFunction<MarketingUserBehavior> {

    // 定义是否运行的标识符
    Boolean running = true;
    // 定义渠道的集合
    String[] channelSet = {"AppStore", "XiaomiStore", "HuaweiStore", "weibo", "wechat", "tieba"};
    // 定义用户行为的集合
    String[] behaviorTypes = {"BROWSE", "CLICK", "PURCHASE", "UNINSTALL"};
    // 定义随机数发生器
    Random rand = new Random();

    @Override
    public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {

        long count = 0;
        long max = Long.MAX_VALUE;

        while (count<max && running) {
            String userId = String.valueOf(rand.nextLong());
            String behaviorType = behaviorTypes[rand.nextInt(behaviorTypes.length)];
            String channel = channelSet[rand.nextInt(channelSet.length)];
            long timestamp = System.currentTimeMillis();
            ctx.collect(new MarketingUserBehavior(userId,behaviorType,channel,timestamp));
            count++;
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}

class MarketingCountByChannel extends ProcessWindowFunction<Tuple2<Tuple2<String,String>,Long>,MarketingViewCount,Tuple2<String,String>, TimeWindow> {

    @Override
    public void process(Tuple2<String, String> stringStringTuple2, Context context, Iterable<Tuple2<Tuple2<String, String>, Long>> elements, Collector<MarketingViewCount> out) throws Exception {

        // 根据 context 对象分别获取到 Long 类型的 窗口的开始和结束时间
        //context.window.getStart是长整形   所以new 一个 变成String类型
        String startTs = String.valueOf(context.window().getStart());
        String endTs = String.valueOf(context.window().getEnd());

        // 获取到 渠道
        String channel = stringStringTuple2.f0;
        // 获取到 行为
        String behaviorType = stringStringTuple2.f1;
        // 获取到 次数
        long count = 0;
        for (Tuple2<Tuple2<String, String>, Long> element : elements) {
            count++;
        }

        // 输出结果
        out.collect(new MarketingViewCount(startTs, endTs, channel, behaviorType, count));
    }
}
