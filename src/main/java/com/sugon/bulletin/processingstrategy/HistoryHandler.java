package com.sugon.bulletin.processingstrategy;

public interface HistoryHandler {
    void handle(int maxDay);
    void destroy();
}
