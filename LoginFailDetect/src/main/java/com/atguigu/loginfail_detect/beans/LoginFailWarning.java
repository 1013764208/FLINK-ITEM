package com.atguigu.loginfail_detect.beans;

public class LoginFailWarning {
    private Long userId;
    private Long firstFailTime;
    private Long lastFileTime;
    private String warningMsg;

    public LoginFailWarning() {
    }

    public LoginFailWarning(Long userId, Long firstFailTime, Long lastFileTime, String warningMsg) {
        this.userId = userId;
        this.firstFailTime = firstFailTime;
        this.lastFileTime = lastFileTime;
        this.warningMsg = warningMsg;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getFirstFailTime() {
        return firstFailTime;
    }

    public void setFirstFailTime(Long firstFailTime) {
        this.firstFailTime = firstFailTime;
    }

    public Long getLastFileTime() {
        return lastFileTime;
    }

    public void setLastFileTime(Long lastFileTime) {
        this.lastFileTime = lastFileTime;
    }

    public String getWarningMsg() {
        return warningMsg;
    }

    public void setWarningMsg(String warningMsg) {
        this.warningMsg = warningMsg;
    }

    @Override
    public String toString() {
        return "LoginFailWarning{" +
                "userId=" + userId +
                ", firstFailTime=" + firstFailTime +
                ", lastFileTime=" + lastFileTime +
                ", warningMsg='" + warningMsg + '\'' +
                '}';
    }
}
