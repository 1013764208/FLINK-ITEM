package com.atguigu.market_analysis;

import com.atguigu.market_analysis.beans.AdClickEvent;
import com.atguigu.market_analysis.beans.AdCountViewByProvince;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.sql.Timestamp;

public class AdsStatisticsByProvince {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);


        // 1.从文件中读取数据
        URL resource = AdsStatisticsByProvince.class.getResource("/AdClickLog.csv");
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

        // 2.基于省份分组，开窗聚合
        SingleOutputStreamOperator<AdCountViewByProvince> adCountResultStream = adClickEventStream.keyBy(AdClickEvent::getProvince)
                .timeWindow(Time.hours(1), Time.minutes(5))  // 定义滑窗，5 分钟输出一次
                .aggregate(new AdCountAgg(), new AdCountResult());

        adCountResultStream.print();

        env.execute("ad count by province job");
    }

    // 自定义预聚合
    // IN就是聚合函数的输入类型，ACC是存储中间结果的类型，OUT是聚合函数的输出类型
    private static class AdCountAgg implements AggregateFunction <AdClickEvent,Long,Long> {

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
    private static class AdCountResult implements WindowFunction<Long, AdCountViewByProvince,String, TimeWindow> {
        @Override
        public void apply(String province, TimeWindow window, Iterable<Long> input, Collector<AdCountViewByProvince> out) throws Exception {
            String windowEnd = new Timestamp(window.getEnd()).toString();
            Long count = input.iterator().next();
            out.collect(new AdCountViewByProvince(province,windowEnd,count));
        }
    }
}
