package com.atguigu.loginfail_detect;

import com.atguigu.loginfail_detect.beans.LoginEvent;
import com.atguigu.loginfail_detect.beans.LoginFailWarning;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;
import java.util.List;
import java.util.Map;

public class LoginFailWithCep {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        // 1.从文件中读取数据
        URL resource = LoginFailWithCep.class.getResource("/LoginLog.csv");
        DataStream<LoginEvent> loginEventStream = env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new LoginEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(LoginEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });


        /* watermark 没到之前所有的数据缓存起来，等到watermark涨到这个时间点，他才处理数据 */
        // 1.定义匹配模式
        // firstFail -> secondFail, within 2s
        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern.<LoginEvent>begin("firstFail").where(new SimpleCondition<LoginEvent>() {    // 第一次登录失败
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getLoginState());
            }
        }).next("secondFail").where(new SimpleCondition<LoginEvent>() {   // 第二次登录失败
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getLoginState());
            }
        }).within(Time.seconds(2));

        // 2.将匹配模式应用到数据流上，得到 pattern stream
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventStream.keyBy(LoginEvent::getUserId), loginFailPattern);

        // 3.检出符合匹配条件的复杂事件，进行转换处理，得到报警信息
        SingleOutputStreamOperator<LoginFailWarning> warningStream = patternStream.select(new LoginFailMatchDetectWarning());


        env.execute("login fail detect with cep job");


    }

    // 实现自定义的 PatternSelectFunction
    private static class LoginFailMatchDetectWarning implements PatternSelectFunction<LoginEvent,LoginFailWarning> {
        @Override
        public LoginFailWarning select(Map<String, List<LoginEvent>> pattern) throws Exception {
            LoginEvent firstFailEvent = pattern.get("firstFail").iterator().next();
            LoginEvent secondFailEvent = pattern.get("secondFail").get(0);
            return new LoginFailWarning(firstFailEvent.getUserId(), firstFailEvent.getTimestamp(), secondFailEvent.getTimestamp(), "login fail 2 times");
        }
    }
}

