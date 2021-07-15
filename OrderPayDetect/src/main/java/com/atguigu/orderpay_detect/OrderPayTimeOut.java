package com.atguigu.orderpay_detect;

import com.atguigu.orderpay_detect.beans.OrderEvent;
import com.atguigu.orderpay_detect.beans.OrderResult;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.util.List;
import java.util.Map;

public class OrderPayTimeOut {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 读取数据并转换成 POJO 类型
        URL resource = OrderPayTimeOut.class.getResource("/OrderLog.csv");
        DataStream<OrderEvent> orderEventStream = env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        // 1.定义带时间限制的模式
        Pattern<OrderEvent, OrderEvent> orderPayPattern = Pattern.<OrderEvent>begin("create").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "create".equals(value.getEventType());
            }
        }).followedBy("pay").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "pay".equals(value.getEventType());
            }
        }).within(Time.minutes(15));


        // 2.定义侧输出流标签，用来表示超时事件
        OutputTag<OrderResult> orderTimeoutTag = new OutputTag<OrderResult>("order-timeout") {};

        // 3.将pattern应用在输入流数据流上，得到 pattern stream
        PatternStream<OrderEvent> patternStream = CEP.pattern(orderEventStream.keyBy(OrderEvent::getOrderId), orderPayPattern);

        // 4.调用 select 方法，实现对匹配复杂事件和超时事件的提取和处理
        SingleOutputStreamOperator<OrderResult> resultStream = patternStream.select(orderTimeoutTag, new OrderTimeOutSelect(), new OrderPaySelect());

        resultStream.print("payed normally");
        resultStream.getSideOutput(orderTimeoutTag).print("timeout");

        env.execute(" ");

    }

    // 实现自定义的超时事件处理函数
    private static class OrderTimeOutSelect implements PatternTimeoutFunction<OrderEvent,OrderResult> {
        @Override
        public OrderResult timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
            Long timeoutOrderId = pattern.get("create").iterator().next().getOrderId();
            return new OrderResult(timeoutOrderId, "timeout" + timeoutTimestamp);
        }
    }

    private static class OrderPaySelect implements PatternSelectFunction<OrderEvent,OrderResult> {
        @Override
        public OrderResult select(Map<String, List<OrderEvent>> pattern) throws Exception {
            Long payedOrderId = pattern.get("create").iterator().next().getOrderId();
            return new OrderResult(payedOrderId,"success pay");
        }
    }
}
