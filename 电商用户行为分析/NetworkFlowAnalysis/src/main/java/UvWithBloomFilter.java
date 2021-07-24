import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * @Description: java类作用描述
 * @Author: Xiaoyuyu
 * @CreateDate: 2021/7/24 2:56 下午
 */
public class UvWithBloomFilter {

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
                .map(new MapFunction<UserBehavior, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(UserBehavior userBehavior) throws Exception {
                        return new Tuple2<>("dummyKey",userBehavior.userId);
                    }
                })
                .keyBy(value->value.f0)
                .timeWindow(Time.minutes(60))
                // 我们不应该等待窗口关闭才去做 Redis 的连接 -》 数据量可能很大，窗口的内存放不下
                // 所以这里使用了 触发窗口操作的API -- 触发器 trigger
                .trigger(new MyTrigger())
                .process(new UvCountWithBloom())
                .print();

        env.execute();
    }
}

class MyTrigger extends Trigger<Tuple2<String, Long>, TimeWindow> {

    @Override
    public TriggerResult onElement(Tuple2<String, Long> stringLongTuple2, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.FIRE_AND_PURGE;
    }

    @Override
    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
    }
}

// 定义一个布隆过滤器
class Bloom {
    public Long cap;

    public Bloom() {
    }

    public Bloom(Long cap) {
        this.cap = cap;
    }

    public Long hash(String value,int seed) {
        long result = 0;
        for(int i=0;i<value.length();i++) {
            result = result * seed + value.charAt(i);
        }
        return result & (cap-1);
    }

    @Override
    public String toString() {
        return "Bloom{" +
                "cap=" + cap +
                '}';
    }
}

class UvCountWithBloom extends ProcessWindowFunction<Tuple2<String, Long>, UVCount, String, TimeWindow> {

    // 可以在这里提前定义和redis的链接，这里我就不定义了
    // 这里我们就定义一个map来表示，其实应该是利用redis的位图，本部分建议还是看原始仓库scala的代码
    public Map<Long,Integer> map = new HashMap<>();

    // 定义bloom过滤器
    public Bloom bloom;

    @Override
    public void open(Configuration parameters) throws Exception {
        bloom = new Bloom(100L);
    }

    @Override
    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<UVCount> out) {

        // 因为是每来一条数据就判断一次，所以我们就可以直接用last获取到这条数据
        String userId = elements.iterator().next().f1.toString();
        // 计算哈希
        long hash = bloom.hash(userId, 61);
        // 定义一个标志位，判断 redis 位图中有没有这一位
        if (!map.containsKey(hash)) {
            map.put(hash, map.getOrDefault(hash,1));
        }
        map.put(hash, map.get(hash)+1);
        out.collect(new UVCount(Long.parseLong(userId),map.get(hash)));
    }
}
