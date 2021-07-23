import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
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
import java.time.Duration;
import java.util.*;

/**
 * @Description: 热门时事商品统计
 * @Author: Xiaoyuyu
 * @CreateDate: 2021/7/18 3:42 下午
 */

// input structure
class UserBehavior {
    public long userId;
    public long itemId;
    public int categoryId;
    public String behavior;
    public long timeStamp;

    public UserBehavior() {
    }

    public UserBehavior(long userId, long itemId, int categoryId, String behavior, long timeStamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId=" + userId +
                ", itemId=" + itemId +
                ", categoryId=" + categoryId +
                ", behavior='" + behavior + '\'' +
                ", timeStamp=" + timeStamp +
                '}';
    }
}

// output structure
class ItemViewCount{
    public long itemID;
    public long windowEnd;
    public long count;

    public ItemViewCount() {
    }

    public ItemViewCount(long itemID, long windowEnd, long count) {
        this.itemID = itemID;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    @Override
    public String toString() {
        return "ItemViewCount{" +
                "itemID=" + itemID +
                ", windowEnd=" + windowEnd +
                ", count=" + count +
                '}';
    }
}


public class HotItems {

    public static void main(String[] args) throws Exception {

        // 定义流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        // 设置时间特征为事件事件
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // source
        // input data
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("data/UserBehavior.csv");

        // transform
        // 将原始数据变成UserBehavior类型
        SingleOutputStreamOperator<UserBehavior> userBehaviorStream = stringDataStreamSource.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String s) throws Exception {
                String[] split = s.split(",");
                return new UserBehavior(Long.parseLong(split[0]), Long.parseLong(split[1]), Integer.parseInt(split[2]), split[3], Long.parseLong(split[4]));
            }
        });
        // 设置水印，处理乱序数据
        // 水印策略，有界无序，定义一个固定延迟事件
        // 同时时间的语义，由我们对象中的timeStamp指定
        SingleOutputStreamOperator<UserBehavior> userBehaviorWatermark = userBehaviorStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, timeStamp) -> event.timeStamp * 1000));
        // 过滤出pv数据
        userBehaviorWatermark.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior userBehavior) throws Exception {
                return userBehavior.behavior.equals("pv");
            }
        })
                // 按照itemId聚合
        .keyBy(value -> value.itemId)
        // Windows can be defined on already partitioned KeyedStreams
        // 定义滑动窗口
        .timeWindow(Time.hours(1), Time.minutes(5))
        // 统计出每种商品的个数，自定义聚合规则，和输出结构
        .aggregate(new CountAgg(), new WindowResult())

        // 按照每次窗口结束时间聚合
        .keyBy(value->value.windowEnd)
        // 输出每个窗口中点击量前N名的商品
        .process(new TopNHotItems(3))
        .print("HotItems");

        env.execute();
    }
}

class CountAgg implements AggregateFunction<UserBehavior,Long,Long> {

    // 定义初始值
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    // 组内规则
    @Override
    public Long add(UserBehavior userBehavior, Long aLong) {
        return aLong+1;
    }

    // 返回值
    @Override
    public Long getResult(Long aLong) {
        return aLong;
    }

    // 组间规则
    @Override
    public Long merge(Long aLong, Long acc1) {
        return aLong+acc1;
    }
}

class WindowResult implements WindowFunction<Long,ItemViewCount,Long, TimeWindow> {

    @Override
    public void apply(Long aLong, TimeWindow timeWindow, java.lang.Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
        collector.collect(new ItemViewCount(aLong,timeWindow.getEnd(),iterable.iterator().next()));
    }
}

class TopNHotItems extends KeyedProcessFunction<Long,ItemViewCount,String> {

    public int n;
    // 定义一个状态变量 list state，用来保存所有的 ItemViewCont
    public ListState<ItemViewCount> itemState;

    public TopNHotItems(int n) {
        this.n = n;
    }

    // // 在执行processElement方法之前，会最先执行并且只执行一次 open 方法
    @Override
    public void open(Configuration parameters) throws Exception {
        itemState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("itemState",ItemViewCount.class));
    }

    @Override
    public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {
        itemState.add(itemViewCount);
        // 注册 windowEnd+1 的 EventTime Timer, 延迟触发，当触发时，说明收齐了属于windowEnd窗口的所有商品数据，统一排序处理
        context.timerService().registerEventTimeTimer(itemViewCount.windowEnd+1);
    }

    // 定时器触发时，会执行这个方法
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // 已经收集到所有的数据，首先把所有的数据放到一个 List 中
        List<ItemViewCount> allItems = new ArrayList<>();
        Iterable<ItemViewCount> itemViewCounts = itemState.get();
        for (ItemViewCount itemViewCount : itemViewCounts) {
            allItems.add(itemViewCount);
        }
        // 清除状态
        itemState.clear();
        // 按照 count 大小  倒序排序
        Collections.sort(allItems, new Comparator<ItemViewCount>() {
            @Override
            public int compare(ItemViewCount o1, ItemViewCount o2) {
                if(o1.count>o2.count) return -1;
                else if(o1.count==o2.count) return 0;
                else return 1;
            }
        });
        List<ItemViewCount> ans = new ArrayList<>();
        int cnt=0;
        while (cnt<n) {
            ans.add(allItems.get(cnt));
            cnt++;
        }
        StringBuilder result = new StringBuilder();
        result.append("======================================================\n");
        // 触发定时器时，我们多设置了1秒的延迟，这里我们将时间减去0.1获取到最精确的时间
        result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n");
        for(ItemViewCount elem:ans) result.append(elem.toString());
        result.append("\n");
        result.append("======================================================\n");
        out.collect(result.toString());
    }
}
