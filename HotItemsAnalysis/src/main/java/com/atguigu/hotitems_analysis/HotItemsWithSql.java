package com.atguigu.hotitems_analysis;

import com.atguigu.hotitems_analysis.beans.ItemViewCount;
import com.atguigu.hotitems_analysis.beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.omg.CORBA.Environment;

import java.util.Properties;

public class HotItemsWithSql {

    public static void main(String[] args) throws Exception {

        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 读取数据，创建 DataStream
        DataStream<String> inputStream = env.readTextFile("D:\\JetBrains\\idea_space\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv");

        // 3. 转换成 POJO，分配时间戳和 watermark
        DataStream<UserBehavior> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {   // 指定时间戳
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000L;
            }
        });

        // 4. 创建表执行环境，用 blink 版本
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 5. 将流转换成表
        Table dataTable = tableEnv.fromDataStream(dataStream, "itemId, behavior, timestamp.rowtime as ts");

        // 6. 分组开窗
        // table api
        Table windowAggTable = dataTable
                .filter("behavior = 'pv'")
                .window(Slide.over("1.hours").every("5.minutes").on("ts").as("w"))
                .groupBy("itemId, w")
                .select("itemId, w.end as windowEnd, itemId.count as cnt");// 相比之前的聚合函数不用在操心，什么都能拿到

        // 7. 利用开窗函数，对 count 值进行排序并获取 row number，得到 Top N
        // SQL
        DataStream<Row> aggStream = tableEnv.toAppendStream(windowAggTable, Row.class);  // 先转成流
        tableEnv.createTemporaryView("agg", aggStream, "itemId, windowEnd, cnt");

        Table resultTable = tableEnv.sqlQuery(" select * from " +
                "( select *, ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num from agg )"
                + "where row_num <= 5");

//        tableEnv.toRetractStream(resultTable, Row.class).print();

        // 上面 API + SQL 繁琐，纯 SQL 实现
        tableEnv.createTemporaryView("data_table",dataStream,"itemId, behavior,timestamp.rowtime as ts");
        Table resultSqlTable = tableEnv.sqlQuery(" select * from " +
                " ( select *, ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num" +
                " from ( " +

                "   select itemId, count(itemId) as cnt, HOP_END(ts, interval '5' minute, interval '1' hour) as windowEnd" +
                "   from data_table " +
                "   where behavior = 'pv' " +
                "   group by itemId, HOP(ts, interval '5' minute, interval '1' hour)" +
                "   )" +
                " )" +
                "where row_num <= 5");

        tableEnv.toRetractStream(resultSqlTable, Row.class).print();

        env.execute("hot items with sql job");

    }
}
