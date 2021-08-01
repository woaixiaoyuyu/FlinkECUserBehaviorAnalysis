import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Description: java类作用描述
 * @Author: Xiaoyuyu
 * @CreateDate: 2021/7/31 10:59 上午
 */
// 定义输入的订单事件样例类
// case class OrderEvent(orderId: Long, eventType: String, eventTime: Long)

// 定义输出的订单检测结果样例类
// case class OrderResult(orderId: Long, resultMsg: String)
class OrderResult {
    public Long orderId;
    public String resultMsg;

    public OrderResult(Long orderId, String resultMsg) {
        this.orderId = orderId;
        this.resultMsg = resultMsg;
    }

    public OrderResult() {
    }

    @Override
    public String toString() {
        return "OrderResult{" +
                "orderId=" + orderId +
                ", resultMsg='" + resultMsg + '\'' +
                '}';
    }
}

class OrderEvent {
    public Long orderId;
    public String eventType;
    public Long eventTime;

    public OrderEvent() {
    }

    public OrderEvent(Long orderId, String eventType, Long eventTime) {
        this.orderId = orderId;
        this.eventType = eventType;
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId=" + orderId +
                ", eventType='" + eventType + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }
}

public class OrderTimeout {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> stringDataStreamSource = env.readTextFile("data/OrderLog.csv");
        SingleOutputStreamOperator<OrderEvent> orderEventStream = stringDataStreamSource.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String s) throws Exception {
                String[] split = s.split(",");
                return new OrderEvent(Long.parseLong(split[0].trim()), split[1].trim(), Long.parseLong(split[3].trim()));
            }
        })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamr) -> event.eventTime * 1000));

        KeyedStream<OrderEvent, Long> keyedStream = orderEventStream.keyBy(value -> value.orderId);

        // 定义匹配模式
        Pattern<OrderEvent, OrderEvent> orderPayPattern = Pattern.<OrderEvent>begin("begin").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent orderEvent) throws Exception {
                return orderEvent.eventType.equals("create");
            }
        }).followedBy("follow").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent orderEvent) throws Exception {
                return orderEvent.eventType.equals("pay");
            }
        }).within(Time.minutes(15));

        // 应用pattern到stream上
        PatternStream<OrderEvent> pattern = CEP.pattern(keyedStream, orderPayPattern);

        // select，提取事件序列，超时的事件要做报警提示
        OutputTag<OrderResult> orderTimeout = new OutputTag<OrderResult>("orderTimeout"){};
        SingleOutputStreamOperator<OrderResult> select = pattern.select(orderTimeout, new OrderTimeoutSelect(), new OrderPaySelect());

        select.print();
        select.getSideOutput(orderTimeout).print();

        env.execute();
    }
}

class OrderTimeoutSelect implements PatternTimeoutFunction<OrderEvent,OrderResult> {

    @Override
    public OrderResult timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
        Long begin = pattern.get("begin").iterator().next().orderId;
        return new OrderResult(begin,"timeout");
    }
}

class OrderPaySelect implements PatternSelectFunction<OrderEvent, OrderResult> {

    @Override
    public OrderResult select(Map<String, List<OrderEvent>> pattern) throws Exception {
        Long follow = pattern.get("follow").iterator().next().orderId;
        return new OrderResult(follow,"payed successfully");
    }
}
