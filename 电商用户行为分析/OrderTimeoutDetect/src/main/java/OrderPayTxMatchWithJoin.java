import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Description: java类作用描述
 * @Author: Xiaoyuyu
 * @CreateDate: 2021/8/2 9:31 上午
 */
public class OrderPayTxMatchWithJoin {

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

        keyedStream.intervalJoin(keyedStream2)
                .between(Time.seconds(-5),Time.seconds(3))
                .process(new OrderPayTxDetectWithJoin()).print();

        env.execute();
    }
}

class OrderPayTxDetectWithJoin extends ProcessJoinFunction<OrderEvent2, ReceiptEvent, Tuple2<OrderEvent2, ReceiptEvent>> {

    @Override
    public void processElement(OrderEvent2 left, ReceiptEvent right, Context ctx, Collector<Tuple2<OrderEvent2, ReceiptEvent>> out) throws Exception {
        out.collect(new Tuple2<>(left,right));
    }
}
