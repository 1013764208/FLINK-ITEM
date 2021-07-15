package com.atguigu.orderpay_detect;

import com.atguigu.orderpay_detect.beans.OrderEvent;
import com.atguigu.orderpay_detect.beans.ReceiptEvent;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;

public class TxPayMatchByJoin {

    // 定义侧输出流标签
    private final static OutputTag<OrderEvent> unmatchedPays = new OutputTag<OrderEvent>("unmatched-pays") {
    };
    private final static OutputTag<ReceiptEvent> unmatchedReceipts = new OutputTag<ReceiptEvent>("unmatched-receipts") {
    };


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 读取订单支付数据
        URL orderResource = TxPayMatchByJoin.class.getResource("/OrderLog.csv");
        DataStream<OrderEvent> orderEventStream = env.readTextFile(orderResource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                }).filter(data -> !"".equals(data.getTxId()));  // 交易ID不为空，必须是 PAY 事件


        // 读取到账事件数据
        URL receiptResource = TxPayMatchByJoin.class.getResource("/ReceiptLog.csv");
        DataStream<ReceiptEvent> receiptEventStream = env.readTextFile(receiptResource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new ReceiptEvent(fields[0], fields[1], new Long(fields[2]));
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ReceiptEvent>() {
                    @Override
                    public long extractAscendingTimestamp(ReceiptEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        // 区间连接两条流，得到匹配数据
        SingleOutputStreamOperator<Tuple2<OrderEvent,ReceiptEvent>> resultStream = orderEventStream
                .keyBy(OrderEvent::getTxId)
                .intervalJoin(receiptEventStream.keyBy(ReceiptEvent::getTxId))
                .between(Time.seconds(-3), Time.seconds(5))  // -3,5 区间范围
                .process(new TxPayMatchDetectByJoin());

        resultStream.print();

        env.execute();
    }

    // 实现自定义 ProcessJoinFunction
    private static class TxPayMatchDetectByJoin extends ProcessJoinFunction<OrderEvent,ReceiptEvent,Tuple2<OrderEvent,ReceiptEvent>> {


        @Override
        public void processElement(OrderEvent left, ReceiptEvent right, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            out.collect(new Tuple2<>(left,right));
        }
    }
}
