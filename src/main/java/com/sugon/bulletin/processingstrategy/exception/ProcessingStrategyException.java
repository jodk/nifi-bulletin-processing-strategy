package com.sugon.bulletin.processingstrategy.exception;

public class ProcessingStrategyException extends RuntimeException {
    public ProcessingStrategyException(String message){
        super(message);
    }
    public ProcessingStrategyException(String message,Throwable e){
        super(message,e);
    }

    public ProcessingStrategyException(Throwable e,String format,Object... params){
        super(String.format(format,params),e);
    }
    public ProcessingStrategyException(String format,Object... params){
        super(String.format(format,params));
    }
}
