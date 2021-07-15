package com.atguigu.networkflow_analysis;

import com.atguigu.networkflow_analysis.beans.ApacheLogEvent;
import com.atguigu.networkflow_analysis.beans.PageViewCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


import java.net.URL;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.regex.Pattern;


public class HotPages {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 读取文件，转换成 POJO
//        URL resource = HotPages.class.getResource("/apache.log");  // 绝对路径太麻烦，使用反射
//        DataStreamSource<String> inputStream = env.readTextFile(resource.getPath());
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);

        DataStream<ApacheLogEvent> dataStream = inputStream
                .map(line -> {
                    String[] fields = line.split(" ");
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                    Long timestamp = simpleDateFormat.parse(fields[3]).getTime();
                    return new ApacheLogEvent(fields[0], fields[1], timestamp, fields[5], fields[6]);
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.minutes(1)) {   // 乱序数据
                    @Override
                    public long extractTimestamp(ApacheLogEvent element) {
                        return element.getTimestamp();
                    }
                });

        // 定义侧输出流标签
        OutputTag<ApacheLogEvent> lateTag = new OutputTag<ApacheLogEvent>("late"){};

        // 分组开窗聚合
        SingleOutputStreamOperator<PageViewCount> windowAggStream = dataStream
                .filter(data -> "GET".equals(data.getMethod()))  // 过滤 get 请求
                .filter(data -> {   // 正则表达式
                    String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                    return Pattern.matches(regex, data.getUrl());
                })
                .keyBy(ApacheLogEvent::getUrl) // 按照 url 分组
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1))  // 允许处理迟到数据，更新之前的结果，之前给一个差不多的结果
                .sideOutputLateData(lateTag)
                .aggregate(new PageCountAgg(), new PageCountResult());

        windowAggStream.print("agg");
        windowAggStream.getSideOutput(lateTag).print("late");


        // 收集同一窗口 count 数据，排序输出
        DataStream<String> resultStream = windowAggStream
                .keyBy(PageViewCount::getWindowEnd)
                .process(new TopNHotPages(3));


        // 打印输出
        resultStream.print();

        env.execute();
    }




    // 自定义预聚合函数
    private static class PageCountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent value, Long accumulator) {
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

    // 实现自定义的窗口函数
    private static class PageCountResult implements WindowFunction<Long, PageViewCount, String, TimeWindow> {
        @Override
        public void apply(String url, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {

            out.collect(new PageViewCount(url, window.getEnd(), input.iterator().next()));
        }
    }

    // 实现自定义的处理函数
    private static class TopNHotPages extends KeyedProcessFunction<Long, PageViewCount, String> {
        private Integer topSize;

        public TopNHotPages(Integer topSize) {
            this.topSize = topSize;
        }

        // 定义状态，保存当前所有 PageViewCount 到 Map
//        ListState<PageViewCount> pageViewCountListState;
        MapState<String,Long> pageViewCountMapState;

        @Override
        public void open(Configuration parameters) throws Exception {
//            pageViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<PageViewCount>("page-count-list", PageViewCount.class));
            pageViewCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("page-count-set", String.class, Long.class));
        }



        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<String> out) throws Exception {
//            pageViewCountListState.add(value);
            pageViewCountMapState.put(value.getUrl(),value.getCount());
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);  // 注册定时器
            // 注册一分钟之后的定时器，用于清空状态
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 60 * 1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 先判断是否到了窗口关闭清理时间，如果是，直接情况状态返回
            if (timestamp == ctx.getCurrentKey() + 60 * 1000L) {
                pageViewCountMapState.clear();
                return;
            }

            ArrayList<Map.Entry<String, Long>> pageViewCounts = Lists.newArrayList(pageViewCountMapState.entries().iterator());
            pageViewCounts.sort(new Comparator<Map.Entry<String, Long>>() {
                @Override
                public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                    if (o1.getValue() > o2.getValue())
                        return -1;
                    else if (o1.getValue() < o2.getValue())
                        return 1;
                    else return 0;
                }
            });

            // 格式化成 String 输出
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("=============================================\n");
            resultBuilder.append("窗口结果时间：").append(new Timestamp(timestamp - 1)).append("\n");

            // 遍历列表，取 top n 输出
            for (int i = 0; i < Math.min(topSize,pageViewCounts.size()); i++) {
                Map.Entry<String, Long> currentItemViewCount = pageViewCounts.get(i);
                resultBuilder.append("NO ").append(i + 1).append(":")
                        .append(" 页面URL = ").append(currentItemViewCount.getKey())
                        .append(" 浏览量 = ").append(currentItemViewCount.getValue())
                        .append("\n");
            }
            resultBuilder.append("============================\n\n");

            // 控制输出频率
            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());

        }
    }
}

