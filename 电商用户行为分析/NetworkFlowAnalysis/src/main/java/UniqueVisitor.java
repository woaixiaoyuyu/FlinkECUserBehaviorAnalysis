import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

/**
 * @Description: java类作用描述
 * @Author: Xiaoyuyu
 * @CreateDate: 2021/7/23 10:41 下午
 */

class UVCount {
    public Long windowEnd;
    public int count;

    public UVCount(Long windowEnd, int count) {
        this.windowEnd = windowEnd;
        this.count = count;
    }

    @Override
    public String toString() {
        return "UVCount{" +
                "windowEnd=" + windowEnd +
                ", count=" + count +
                '}';
    }
}

public class UniqueVisitor {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> stringDataStreamSource = env.readTextFile("data/UserBehavior.csv");

        SingleOutputStreamOperator<UserBehavior> userBehaviorStream = stringDataStreamSource.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String s) throws Exception {
                String[] split = s.split(",");
                return new UserBehavior(Long.parseLong(split[0]), Long.parseLong(split[1]), Integer.parseInt(split[2]), split[3], Long.parseLong(split[4]));
            }
        });

        SingleOutputStreamOperator<UserBehavior> userBehaviorWatermark = userBehaviorStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, timeStamp) -> event.timeStamp * 1000));

        userBehaviorWatermark.filter(value -> value.behavior.equals("pv"))
                .timeWindowAll(Time.minutes(60))
//                .apply(new UvCountByWindow())
                .process(new UvCountByProcess())
                .print();

        env.execute();
    }
}

class UvCountByWindow implements AllWindowFunction<UserBehavior,UVCount, TimeWindow> {

    @Override
    public void apply(TimeWindow window, Iterable<UserBehavior> input, Collector<UVCount> out) {
        Set<Long> longs = new HashSet<>();
        for (UserBehavior next : input) {
            longs.add(next.userId);
        }
        out.collect(new UVCount(window.getEnd(),longs.size()));
    }
}

class UvCountByProcess extends ProcessAllWindowFunction<UserBehavior, UVCount, TimeWindow> {

    public MapState<Long, Long> mapState;
    public int cnt = 0;

    @Override
    public void open(Configuration parameters) throws Exception {
        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("mapState",Long.class , Long.class));
    }

    @Override
    public void process(Context context, Iterable<UserBehavior> iterable, Collector<UVCount> collector) throws Exception {
        for(UserBehavior elem:iterable) {
            if(!mapState.contains(elem.userId)) {
                mapState.put(elem.userId,1L);
                cnt++;
            }
        }
        collector.collect(new UVCount(context.window().getEnd(),cnt));
        mapState.clear();
    }


}
