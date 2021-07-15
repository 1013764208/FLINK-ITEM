package com.atguigu.networkflow_analysis;

import com.atguigu.networkflow_analysis.beans.PageViewCount;
import com.atguigu.networkflow_analysis.beans.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
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
import scala.util.parsing.input.Page;


import java.net.URL;
import java.util.Random;

public class PageView {
    public static void main(String[] args) throws Exception {

        // 1.创建你执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2.读取数据，创建 DataStream
        URL resource = PageView.class.getResource("/UserBehavior.csv");
        DataStream<String> inputStream = env.readTextFile(resource.getPath());

        // 3.转换成 POJO，分配时间戳和 watermark
        DataStream<UserBehavior> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {   // 指定时间戳
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000L;
            }
        });

        // 4.分组开窗聚合，得到每个窗口内各个窗口的 count 值
        DataStream<Tuple2<String, Long>> pvResultStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior()))  // 过滤 pv 行为
                .map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                        return new Tuple2<>("pv", 1L);
                    }
                })
                .keyBy(0) // 分组  / 索引位置
                .timeWindow(Time.hours(1))      // 开 1 小时滚动窗口
                .sum(1);// 索引位置

        // 并行任务改进，设计随机 key ，解决数据倾斜问题
        SingleOutputStreamOperator<PageViewCount> pvStream = dataStream.filter(data -> "pv".equals(data.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> map(UserBehavior value) throws Exception {
                        Random random = new Random();
                        return new Tuple2<>(random.nextInt(10), 1L);
                    }
                })
                .keyBy(data -> data.f0)
                .timeWindow(Time.hours(1))
                .aggregate(new PvCountAgg(), new PvCountResult());

        // 将可分区的数据汇总起来
        DataStream<PageViewCount> PageViewCount = pvStream
                .keyBy(com.atguigu.networkflow_analysis.beans.PageViewCount::getWindowEnd)
                .process(new TotalPvCount());
//                .sum("count");



        pvStream.print();

        env.execute("pv count job");
    }

    // 实现自定义预聚合函数
    private static class PvCountAgg implements AggregateFunction<Tuple2<Integer,Long>,Long,Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<Integer, Long> value, Long accumulator) {
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

    // 实现自定义窗口函数
    private static class PvCountResult implements WindowFunction<Long,PageViewCount,Integer, TimeWindow> {
        @Override
        public void apply(Integer integer, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
            out.collect(new PageViewCount(integer.toString(),window.getEnd(),input.iterator().next()));
        }
    }

    // 实现自定义处理函数，把相同窗口分组统计的 count 值叠加
    private static class TotalPvCount extends KeyedProcessFunction<Long, PageViewCount,PageViewCount> {
        // 定义状态，保存当前的总 count 值
        ValueState<Long> totalCountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            totalCountState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("total-count", Long.class,0L));
        }

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<PageViewCount> out) throws Exception {
            // 更新状态
            totalCountState.update(totalCountState.value() + value.getCount());
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {
            // 定时器触发，所有分组 count 值都到齐，直接输出当前的总的 count 数量
            Long totalCount = totalCountState.value();
            out.collect(new PageViewCount("pv",ctx.getCurrentKey(),totalCount));

            // 清空状态
            totalCountState.clear();
        }
    }
}
