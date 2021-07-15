package com.atguigu.loginfail_detect;

import com.atguigu.loginfail_detect.beans.LoginEvent;
import com.atguigu.loginfail_detect.beans.LoginFailWarning;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;

public class LoginFailPlus {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        // 1.从文件中读取数据
        URL resource = LoginFailPlus.class.getResource("/LoginLog.csv");
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

        // 自定义处理函数检测连续登录失败事件
        SingleOutputStreamOperator<LoginFailWarning> warningStream = loginEventStream.keyBy(LoginEvent::getUserId)
                .process(new LoginFailDetectWarning(2));

        warningStream.print();
        env.execute();


    }

    // 实现自定义 KeyedProcessFunction
    private static class LoginFailDetectWarning extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {
        // 定义属性，最大连续登录失败次数
        private Integer maxFailTimes;

        // 定义状态：保存2秒内所有的失败事件
        ListState<LoginEvent> loginFailEventListState;

        public LoginFailDetectWarning(int maxFailTimes) {
            this.maxFailTimes = maxFailTimes;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            loginFailEventListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("login-fail-list", LoginEvent.class));
        }

        // 以登录事件作为判断报警的触发条件，不再注册定时器
        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<LoginFailWarning> out) throws Exception {
            // 判断当前事件登录状态
            if ("fail".equals(value.getLoginState())) {
                // 1.如果是登录失败，获取状态中之前的登录失败事件，继续判断是否已有失败事件
                Iterator<LoginEvent> iterator = loginFailEventListState.get().iterator();
                if (iterator.hasNext()) {
                    // 1.1 如果已经有登录失败事件，继续判断时间戳是否在2秒内
                    // 获取已有的登录失败事件
                    LoginEvent firstFailEvent = iterator.next();
                    if (value.getTimestamp() - firstFailEvent.getTimestamp() <= 2) {
                        // 1.1.1 如果在2秒之内，输出报警
                        out.collect(new LoginFailWarning(value.getUserId(),firstFailEvent.getTimestamp(),value.getTimestamp(),"login fail 2 time in 2s"));
                    }

                    // 不管报不报警，这次都已经处理完毕，直接更新状态
                    loginFailEventListState.clear();
                    loginFailEventListState.add(value);
                } else {
                    // 1.2 如果没有登录失败，直接将当前事件存入 ListState
                    loginFailEventListState.add(value);
                }
            } else {
                // 2.如果是登录成功， 直接清空状态
            }

        }
    }
}
