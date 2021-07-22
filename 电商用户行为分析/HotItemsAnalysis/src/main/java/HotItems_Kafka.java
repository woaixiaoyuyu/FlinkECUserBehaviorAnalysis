import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;

/**
 * @Description: 热门时事商品统计
 * @Author: Xiaoyuyu
 * @CreateDate: 2021/7/18 3:42 下午
 */


public class HotItems_Kafka {

    public static void main(String[] args) throws Exception {

        // 定义流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        // 设置时间特征为事件事件
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // source
        // input data
        // create kafka properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","192.168.166.200:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        DataStreamSource<String> stringDataStreamSource = env.addSource(new FlinkKafkaConsumer<>("hotItem", new SimpleStringSchema(), properties));
//        DataStreamSource<String> stringDataStreamSource = env.readTextFile("data/UserBehavior.csv");

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

