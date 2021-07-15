package com.atguigu.market_analysis;

import com.atguigu.market_analysis.beans.AdClickEvent;
import com.atguigu.market_analysis.beans.AdCountViewByProvince;
import com.atguigu.market_analysis.beans.BlackListUserWarning;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.Int;

import java.net.FileNameMap;
import java.net.URL;
import java.sql.Timestamp;

public class AdsStatisticsByProvinceBlackWords {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);


        // 1.从文件中读取数据
        URL resource = AdsStatisticsByProvinceBlackWords.class.getResource("/AdClickLog.csv");
        DataStream<AdClickEvent> adClickEventStream = env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new AdClickEvent(new Long(fields[0]), new Long(fields[1]), fields[2], fields[3], new Long(fields[4]));
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickEvent>() {
                    @Override
                    public long extractAscendingTimestamp(AdClickEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        // 2.对同一个用户点击同一个广告进行检测报警
        SingleOutputStreamOperator<AdClickEvent> filterAdClickStream = adClickEventStream
                .keyBy("userId", "adId")  // 组合键，基于用户id和广告id做分组
                .process(new FilterBlackListUser(100));

        // 3.基于省份分组，开窗聚合
        SingleOutputStreamOperator<AdCountViewByProvince> adCountResultStream = filterAdClickStream.keyBy(AdClickEvent::getProvince)
                .timeWindow(Time.hours(1), Time.minutes(5))  // 定义滑窗，5 分钟输出一次
                .aggregate(new AdCountAgg(), new AdCountResult());

        adCountResultStream.print();
        filterAdClickStream.getSideOutput(new OutputTag<BlackListUserWarning>("blacklist"){}).print("blacklist-user");

        env.execute("ad count by province job");
    }

    // 自定义预聚合
    // IN就是聚合函数的输入类型，ACC是存储中间结果的类型，OUT是聚合函数的输出类型
    private static class AdCountAgg implements AggregateFunction<AdClickEvent, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    // 自定义全量窗口函数
    private static class AdCountResult implements WindowFunction<Long, AdCountViewByProvince, String, TimeWindow> {
        @Override
        public void apply(String province, TimeWindow window, Iterable<Long> input, Collector<AdCountViewByProvince> out) throws Exception {
            String windowEnd = new Timestamp(window.getEnd()).toString();
            Long count = input.iterator().next();
            out.collect(new AdCountViewByProvince(province, windowEnd, count));
        }
    }

    // 实现自定义处理函数
    private static class FilterBlackListUser extends KeyedProcessFunction<Tuple, AdClickEvent, AdClickEvent> {
        // 定义属性: 点击次数上限
        private Integer countUpperBound;
        // 定义状态，保存当前用户对某一广告的点击次数
        ValueState<Long> countStates;
        // 定义标志状态，保存当前用户是否已经被发送到黑名单里
        ValueState<Boolean> isSentState;

        public FilterBlackListUser(Integer countUpperBound) {
            this.countUpperBound = countUpperBound;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            countStates = getRuntimeContext().getState(new ValueStateDescriptor<Long>("adcount", Long.class, 0L));
            isSentState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-sent", Boolean.class, false));
        }

        @Override
        public void processElement(AdClickEvent value, Context ctx, Collector<AdClickEvent> out) throws Exception {
            // 判断当前用户对同一广告的点击次数，如果不够上限，就 count + 1 正常输出；如果达到上限。直接过滤掉，并侧流输出黑名单报警
            // 首先获取当前的count值
            Long curCount = countStates.value();

            // 1.判断是否是第一个数据，如果是的话，注册一个第二天0点的定时器
            if (curCount == 0) {
                Long ts = (ctx.timerService().currentProcessingTime() / (24 * 60 * 60 * 1000) + 1) * (24 * 60 * 60 * 1000) - 8 * 60 * 60 * 1000;
                ctx.timerService().registerEventTimeTimer(ts);
            }

            // 2.判断是否报警
            if (curCount >= countUpperBound) {
                // 判断是否输出到黑名单过，如果没有的话就输出到侧输出流
                if (!isSentState.value()) {
                    isSentState.update(true);  // 更新状态
                    ctx.output(new OutputTag<BlackListUserWarning>("blacklist"){},
                            new BlackListUserWarning(value.getUserId(), value.getAdId(), "click over" + countUpperBound + "times."));
                }

                return; // 不再执行下面操作
            }

            // 如果没有返回，点击次数 + 1，更新状态，正常输出当前数据到主流
            countStates.update(curCount + 1);
            out.collect(value);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
            // 清空所有状态
            countStates.clear();
            isSentState.clear();
        }
    }
}
