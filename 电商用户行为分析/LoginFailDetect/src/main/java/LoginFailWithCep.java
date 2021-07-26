import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Description: java类作用描述
 * @Author: Xiaoyuyu
 * @CreateDate: 2021/7/26 5:34 下午
 */
public class LoginFailWithCep {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> stringDataStreamSource = env.readTextFile("data/LoginLog.csv");
        KeyedStream<LoginEvent, Long> loginEventStream = stringDataStreamSource.map(new MapFunction<String, LoginEvent>() {
            @Override
            public LoginEvent map(String s) throws Exception {
                String[] split = s.split(",");
                return new LoginEvent(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]));
            }
        })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamr) -> event.eventTime * 1000))
                .keyBy(value -> value.userId);

        // 定义匹配的模式
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("begin").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent) throws Exception {
                return loginEvent.eventType.equals("fail");
            }
        })
                .next("next")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.eventType.equals("fail");
                    }
                })
                .within(Time.seconds(2));

        // 将 pattern 应用到 输入流 上，得到一个 pattern stream
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventStream, pattern);

        // 用 select 方法检出 符合模式的事件序列
        SingleOutputStreamOperator<Warning> select = patternStream.select(new LoginFailMatch());
        select.print();

        env.execute();
    }
}

class LoginFailMatch implements PatternSelectFunction<LoginEvent,Warning> {

    @Override
    public Warning select(Map<String, List<LoginEvent>> map) throws Exception {
        LoginEvent begin = map.get("begin").iterator().next();
        LoginEvent next = map.get("next").iterator().next();
        return new Warning(begin.userId,begin.eventTime,next.eventTime,"在2秒内连续2次登录失败。");
    }
}
