import com.google.gson.internal.$Gson$Preconditions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

/**
 * @Description: java类作用描述
 * @Author: Xiaoyuyu
 * @CreateDate: 2021/7/22 11:14 下午
 */

class ApacheLogEvent {
    public String ip;
    public String userId;
    public long eventTime;
    public String method;
    public String url;

    public ApacheLogEvent(String ip, String userId, long eventTime, String method, String url) {
        this.ip = ip;
        this.userId = userId;
        this.eventTime = eventTime;
        this.method = method;
        this.url = url;
    }

    @Override
    public String toString() {
        return "ApacheLogEvent{" +
                "ip='" + ip + '\'' +
                ", userId='" + userId + '\'' +
                ", eventTime=" + eventTime +
                ", method='" + method + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}

class UrlViewCount {
    public String url;
    public long windowEnd;
    public long count;

    public UrlViewCount(String url, long windowEnd, long count) {
        this.url = url;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    @Override
    public String toString() {
        return "UrlViewCount{" +
                "url='" + url + '\'' +
                ", windowEnd=" + windowEnd +
                ", count=" + count +
                '}';
    }
}

public class NetworkFlow {

    public static void main(String[] args) throws Exception {
        // 每隔5秒，输出最近10分钟内访问量最多的前N个URL

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置时间特征为事件事件
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 38.99.236.50 - - 20/05/2015:21:05:31 +0000 GET /favicon.ico
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("data/apache.log");
        // transform to ApacheLogEvent
        SingleOutputStreamOperator<ApacheLogEvent> mapStream = stringDataStreamSource.map(new MapFunction<String, ApacheLogEvent>() {
            @Override
            public ApacheLogEvent map(String s) throws Exception {
                String[] s1 = s.split(" ");
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                Date parse = simpleDateFormat.parse(s1[3].trim());
                long time = parse.getTime();
//                System.out.println(time);
                return new ApacheLogEvent(s1[0].trim(), s1[1].trim(), time, s1[5].trim(), s1[6].trim());
            }
        });
        // set watermark
        SingleOutputStreamOperator<ApacheLogEvent> logWatermark = mapStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<ApacheLogEvent>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                .withTimestampAssigner((event, timeStamp) -> event.eventTime));

        logWatermark.keyBy(value->value.url)
                .timeWindow(Time.minutes(10),Time.seconds(5))
                .aggregate(new CountAgg(),new WindowResult())
                .keyBy(value->value.windowEnd)
                .process(new TopNHotUrls(5))
                .print("print");

        env.execute();
    }
}

class CountAgg implements AggregateFunction<ApacheLogEvent,Long,Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(ApacheLogEvent apacheLogEvent, Long aLong) {
        return aLong+1;
    }

    @Override
    public Long getResult(Long aLong) {
        return aLong;
    }

    @Override
    public Long merge(Long aLong, Long acc1) {
        return aLong+acc1;
    }
}

class WindowResult implements WindowFunction<Long,UrlViewCount,String,TimeWindow> {

    @Override
    public void apply(String s, TimeWindow timeWindow, Iterable<Long> iterable, Collector<UrlViewCount> collector) throws Exception {
        collector.collect(new UrlViewCount(s,timeWindow.getEnd(),iterable.iterator().next()));
    }
}

class TopNHotUrls extends KeyedProcessFunction<Long,UrlViewCount,String> {

    public int n;
    public ListState<UrlViewCount> urlHot;

    public TopNHotUrls(int n) {
        this.n = n;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        urlHot = getRuntimeContext().getListState(new ListStateDescriptor<UrlViewCount>("urlHot", UrlViewCount.class));
    }

    @Override
    public void processElement(UrlViewCount urlViewCount, Context context, Collector<String> collector) throws Exception {
        urlHot.add(urlViewCount);
        context.timerService().registerEventTimeTimer(urlViewCount.windowEnd+1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        List<UrlViewCount> list = new ArrayList<>();
        Iterable<UrlViewCount> urlViewCounts = urlHot.get();
        for(UrlViewCount elem:urlViewCounts) {
            list.add(elem);
        }
        urlHot.clear();
        Collections.sort(list, new Comparator<UrlViewCount>() {
            @Override
            public int compare(UrlViewCount o1, UrlViewCount o2) {
                if(o1.count>o2.count) return -1;
                else if(o1.count==o2.count) return 0;
                else return 1;
            }
        });
        int cnt = 0;
        List<UrlViewCount> ans = new ArrayList<>();
        while (cnt<n) {
            if(list.isEmpty()) break;
            ans.add(list.get(0));
            list.remove(0);
            cnt++;
        }
        StringBuilder result = new StringBuilder();
        result.append("======================================================\n");
        // 触发定时器时，我们多设置了1秒的延迟，这里我们将时间减去1获取到最精确的时间
        result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n");
        for(UrlViewCount elem:ans) result.append(elem.toString());
        result.append("\n");
        result.append("======================================================\n");
        out.collect(result.toString());
    }
}
