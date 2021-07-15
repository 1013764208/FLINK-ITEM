package com.atguigu.market_analysis.beans;

public class ChannelPromotionCount {
    private String channel;
    private String behavior;
    private String windowEnd;
    private Long count;

    public ChannelPromotionCount() {
    }

    public ChannelPromotionCount(String channel, String behavior, String windowEnd, Long count) {
        this.channel = channel;
        this.behavior = behavior;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    @Override
    public String toString() {
        return "ChannelPromotionCount{" +
                "channel='" + channel + '\'' +
                ", behavior='" + behavior + '\'' +
                ", windowEnd='" + windowEnd + '\'' +
                ", count=" + count +
                '}';
    }
}
