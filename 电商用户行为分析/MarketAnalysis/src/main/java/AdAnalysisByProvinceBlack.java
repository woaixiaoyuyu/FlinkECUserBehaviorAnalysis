import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @Description: java类作用描述
 * @Author: Xiaoyuyu
 * @CreateDate: 2021/7/25 10:26 下午
 */

//定义侧输出流报警信息样例类
// case class BlackListWarning(userId:Long,adId:Long,msg:String)
class BlackListWarning {
    public long userId;
    public long adId;
    public String msg;

    public BlackListWarning(long userId, long adId, String msg) {
        this.userId = userId;
        this.adId = adId;
        this.msg = msg;
    }

    @Override
    public String toString() {
        return "BlackListWarning{" +
                "userId=" + userId +
                ", adId=" + adId +
                ", msg='" + msg + '\'' +
                '}';
    }
}

public class AdAnalysisByProvinceBlack {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStreamSource<String> stringDataStreamSource = env.readTextFile("data/AdClickLog.csv");

        SingleOutputStreamOperator<AdClickEvent> adLogStream = stringDataStreamSource.map(new MapFunction<String, AdClickEvent>() {
            @Override
            public AdClickEvent map(String s) throws Exception {
                String[] s1 = s.split(",");
                return new AdClickEvent(Long.parseLong(s1[0]), Long.parseLong(s1[1]), s1[2], s1[3], Long.parseLong(s1[4]));
            }
        })
        .assignTimestampsAndWatermarks(WatermarkStrategy.<AdClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
        .withTimestampAssigner((event, timestamp) -> event.timestamp * 1000));

        SingleOutputStreamOperator<AdClickEvent> filterBlackListStream = adLogStream.keyBy(new KeySelector<AdClickEvent, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> getKey(AdClickEvent adClickEvent) throws Exception {
                return new Tuple2<>(adClickEvent.userId, adClickEvent.adId);
            }
        })
        .process(new FilterBlackList(100L));

        SingleOutputStreamOperator<String> process = filterBlackListStream
                .keyBy(value -> value.province)
                .timeWindow(Time.minutes(60), Time.seconds(5))
                .process(new AdCount());

//        process.print();

        filterBlackListStream.getSideOutput(new OutputTag<BlackListWarning>("BlackListOutputTag"){}).print();

        env.execute();
    }
}

class FilterBlackList extends KeyedProcessFunction<Tuple2<Long, Long>, AdClickEvent, AdClickEvent> {

    public long cnt;

    public FilterBlackList(long cnt) {
        this.cnt = cnt;
    }

    ValueState<Long> count;
    ValueState<Boolean> state;

    // 定义一个状态，需要保存当前用户对当前广告的点击量 count
    // lazy val countState:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count",classOf[Long]))
    // 定义一个标识位，用来表示用户是否已经在黑名单中
    // lazy val isSendState:ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-sent",classOf[Boolean]))

    @Override
    public void open(Configuration parameters) throws Exception {
        count = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
        state = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-sent", Boolean.class));
    }

    @Override
    public void processElement(AdClickEvent value, Context ctx, Collector<AdClickEvent> out) throws Exception {
        // 取出状态数据
        Long value1 = count.value();
        // 如果是第一个数据，那么注册第二天0点的定时器，用于清空状态
        if(value1==null || value1==0L) {
            count.update(0L);
            state.update(false);
            long ts = (ctx.timerService().currentProcessingTime()/(1000*60*60*24) + 1) * (1000*60*60*24);
            ctx.timerService().registerProcessingTimeTimer(ts);
        }
        // 判断 count 值是否达到上限，如果达到，并且之前没有输出过报警信息，那么则报警
        if(count.value()>cnt) {
            if(!state.value()) {
                // 旁路输出数据
                ctx.output(new OutputTag<BlackListWarning>("BlackListOutputTag"){},new BlackListWarning(value.userId,value.adId,"click over "+cnt+" times today"));
                // 更新黑名单
                state.update(true);
            }
            return;
        }
        count.update(count.value()+1);
        out.collect(value);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
        count.clear();
        state.clear();
    }
}
