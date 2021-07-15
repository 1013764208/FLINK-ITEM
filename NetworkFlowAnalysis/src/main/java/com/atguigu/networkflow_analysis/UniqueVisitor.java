package com.atguigu.networkflow_analysis;

import com.atguigu.networkflow_analysis.beans.PageViewCount;
import com.atguigu.networkflow_analysis.beans.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.swing.tree.AbstractLayoutCache;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;

public class UniqueVisitor {

    public static void main(String[] args) throws Exception {

        // 1.创建你执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

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

        // 开窗统计 uv 值
        SingleOutputStreamOperator<PageViewCount> uvStream = dataStream.filter(data -> "pv".equals(data.getBehavior()))
                .timeWindowAll(Time.hours(1))
                .apply(new UvCountResult());

        uvStream.print();


        env.execute("uv job");
    }

    // 实现自定义全窗口函数
    private static class UvCountResult implements AllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {

        @Override
        public void apply(TimeWindow window, Iterable<UserBehavior> values, Collector<PageViewCount> out) throws Exception {
            // 定义 set 结构，保存所有窗口中的所有的 userId，自动去重
            HashSet<Long> uidSet = new HashSet<>();
            for (UserBehavior ub: values)
                uidSet.add(ub.getUserId());

            out.collect(new PageViewCount("uv",window.getEnd(),(long)uidSet.size()));
        }
    }
}
