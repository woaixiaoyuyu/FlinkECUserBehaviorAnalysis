import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @Description: java类作用描述
 * @Author: Xiaoyuyu
 * @CreateDate: 2021/8/1 8:15 下午
 */

// 输入输出的样例类
//  case class ReceiptEvent(txId:String, payChannel:String, timestamp:Long)
//  case class OrderEvent(orderId:Long, eventType:String, txId:String, eventTime:Long)

class ReceiptEvent {
    public String txId;
    public String payChannel;
    public Long timestamp;

    public ReceiptEvent(String txId, String payChannel, Long timestamp) {
        this.txId = txId;
        this.payChannel = payChannel;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "ReceiptEvent{" +
                "txId='" + txId + '\'' +
                ", payChannel='" + payChannel + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}

class OrderEvent2 {
    public Long orderId;
    public String eventType;
    public String txId;
    public Long eventTime;

    public OrderEvent2(Long orderId, String eventType, String txId, Long eventTime) {
        this.orderId = orderId;
        this.eventType = eventType;
        this.txId = txId;
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "OrderEvent2{" +
                "orderId=" + orderId +
                ", eventType='" + eventType + '\'' +
                ", txId='" + txId + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }
}

public class OrderPayTxMatch {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> stringDataStreamSource = env.readTextFile("data/OrderLog.csv");
        SingleOutputStreamOperator<OrderEvent2> orderEventStream = stringDataStreamSource.map(new MapFunction<String, OrderEvent2>() {
            @Override
            public OrderEvent2 map(String s) throws Exception {
                String[] split = s.split(",");
                return new OrderEvent2(Long.parseLong(split[0].trim()), split[1].trim(), split[2].trim(), Long.parseLong(split[3].trim()));
            }
        })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent2>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamr) -> event.eventTime * 1000));

        KeyedStream<OrderEvent2, String> keyedStream = orderEventStream.keyBy(value -> value.txId);

        DataStreamSource<String> stringDataStreamSource2 = env.readTextFile("data/ReceiptLog.csv");
        SingleOutputStreamOperator<ReceiptEvent> receiptEventStream = stringDataStreamSource2.map(new MapFunction<String, ReceiptEvent>() {
            @Override
            public ReceiptEvent map(String s) throws Exception {
                String[] split = s.split(",");
                return new ReceiptEvent(split[0].trim(), split[1].trim(), Long.parseLong(split[2].trim()));
            }
        })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ReceiptEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamr) -> event.timestamp * 1000));

        KeyedStream<ReceiptEvent, String> keyedStream2 = receiptEventStream.keyBy(value -> value.txId);

        // 定义测输出流
        // OutputTag<OrderEvent2> outputTag = new OutputTag<OrderEvent2>("unmatchedPay") {};
        // OutputTag<ReceiptEvent> outputTag2 = new OutputTag<ReceiptEvent>("unmatchedRec") {};

        // connect 连接两条流，匹配事件进行处理
        SingleOutputStreamOperator<Tuple2<OrderEvent2, ReceiptEvent>> process = keyedStream.connect(keyedStream2).process(new OrderPayTxDetect());

        // 打印输出
        process.print();
        process.getSideOutput(new OutputTag<OrderEvent2>("unmatchedPay") {}).print();
        process.getSideOutput(new OutputTag<ReceiptEvent>("unmatchedRec") {}).print();

        env.execute();
    }
}

class OrderPayTxDetect extends CoProcessFunction<OrderEvent2, ReceiptEvent, Tuple2<OrderEvent2, ReceiptEvent>> {

    public ValueState<OrderEvent2> pay;
    public ValueState<ReceiptEvent> receipt;

    @Override
    public void open(Configuration parameters) throws Exception {
        pay = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent2>("pay", OrderEvent2.class));
        receipt = getRuntimeContext().getState(new ValueStateDescriptor<ReceiptEvent>("receipt", ReceiptEvent.class));

    }

    @Override
    public void processElement1(OrderEvent2 value, Context ctx, Collector<Tuple2<OrderEvent2, ReceiptEvent>> out) throws Exception {
        // pay 来了，考察有没有对应的 receipt 来过
        ReceiptEvent value1 = receipt.value();
        if(value1!=null) {
            // 如果已经有 receipt，正常输出到主流
            out.collect(new Tuple2<>(value,value1));
            receipt.clear();
        } else {
            // 如果 receipt 还没来，那么把 pay 存入状态，注册一个定时器等待 5 秒
            pay.update(value);
            ctx.timerService().registerProcessingTimeTimer(value.eventTime*1000L + 5000L);
        }

    }

    @Override
    public void processElement2(ReceiptEvent value, Context ctx, Collector<Tuple2<OrderEvent2, ReceiptEvent>> out) throws Exception {
        //receipt来了，判断有没有对应的pay来过
        OrderEvent2 value1 = pay.value();
        if(value1!=null) {
            // 如果已经有 pay，正常输出到主流
            out.collect(new Tuple2<>(value1,value));
            pay.clear();
        } else {
            // 如果 pay 还没来，那么把 receipt 存入状态，注册一个定时器等待 3 秒
            receipt.update(value);
            ctx.timerService().registerProcessingTimeTimer(value.timestamp*1000L + 3000L);
        }
    }

    // 定时触发， 有两种情况，所以要判断当前有没有pay和receipt
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent2, ReceiptEvent>> out) throws Exception {
        //如果 pay 不为空，说明receipt没来，输出unmatchedPays
        if (pay.value() != null){
            ctx.output(new OutputTag<OrderEvent2>("unmatchedPay") {},pay.value());
        }

        if (receipt.value() != null){
            ctx.output(new OutputTag<ReceiptEvent>("unmatchedRec") {},receipt.value());
        }

        // 清除状态
        pay.clear();
        receipt.clear();
    }
}
