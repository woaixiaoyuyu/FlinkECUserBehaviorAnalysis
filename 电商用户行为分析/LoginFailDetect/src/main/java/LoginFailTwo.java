import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Iterator;

/**
 * @Description: java类作用描述
 * @Author: Xiaoyuyu
 * @CreateDate: 2021/7/26 1:49 下午
 */

// 输入的登录事件样例类
// case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)
class LoginEvent {
    public long userId;
    public String ip;
    public String eventType;
    public long eventTime;

    public LoginEvent(long userId, String ip, String eventType, long eventTime) {
        this.userId = userId;
        this.ip = ip;
        this.eventType = eventType;
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "LoginEvent{" +
                "userId=" + userId +
                ", ip='" + ip + '\'' +
                ", eventType='" + eventType + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }
}

// 输出的报警信息样例类
// case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)
class Warning {
    public long userId;
    public long firstFailTime;
    public long lastFailTime;
    public String warningMsg;

    public Warning(long userId, long firstFailTime, long lastFailTime, String warningMsg) {
        this.userId = userId;
        this.firstFailTime = firstFailTime;
        this.lastFailTime = lastFailTime;
        this.warningMsg = warningMsg;
    }

    @Override
    public String toString() {
        return "Warning{" +
                "userId=" + userId +
                ", firstFailTime=" + firstFailTime +
                ", lastFailTime=" + lastFailTime +
                ", warningMsg='" + warningMsg + '\'' +
                '}';
    }
}

public class LoginFailTwo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> stringDataStreamSource = env.readTextFile("data/LoginLog.csv");
        stringDataStreamSource.map(new MapFunction<String, LoginEvent>() {
            @Override
            public LoginEvent map(String s) throws Exception {
                String[] split = s.split(",");
                return new LoginEvent(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]));
            }
        })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamr) -> event.eventTime * 1000))
                .keyBy(value -> value.userId)
                .process(new LoginWarning())
                .print();

        env.execute();
    }
}

class LoginWarning extends KeyedProcessFunction<Long, LoginEvent, Warning> {

    ListState<LoginEvent> log;

    @Override
    public void open(Configuration parameters) throws Exception {
        log = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("log", LoginEvent.class));
    }

    @Override
    public void processElement(LoginEvent value, Context ctx, Collector<Warning> out) throws Exception {
        if(value.eventType.equals("fail")) {
            // 先获取之前失败的事件
            Iterator<LoginEvent> iterator = log.get().iterator();
            if(iterator.hasNext()) {
                // 如果之前已经有失败的事件，就做判断，如果没有就把当前失败事件保存进state
                LoginEvent next = iterator.next();
                if (value.eventTime < next.eventTime + 2){
                    out.collect(new Warning( value.userId,next.eventTime,value.eventTime,"在2秒内连续两次登录失败。"));
                }

                // 更新最近一次的登录失败事件，保存在状态里
                log.clear();
            }
            // 如果是第一次登录失败，之前把当前记录 保存至 state
            log.add(value);
        } else {
            // 当前登录状态 不为 fail，则直接清除状态
            log.clear();
        }
    }
}
