package edu.sdsc.queryprocessing.scheduler.utils.metadata;

import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.scheduler.utils.ExecutorEnum;

import java.util.concurrent.CountDownLatch;

// only executor with extra attributes need to extend
public class ExecutorMetaData {
    ExecutorEnum executor;

    private PipelineMode pipelineCap = PipelineMode.block;

    private PipelineMode executionMode = PipelineMode.block;

    private CountDownLatch producer = new CountDownLatch(1);

    public ExecutorMetaData(ExecutorEnum executor) {
        this.executor = executor;
       // this.pipelineCap = cap;
     //   this.executionMode = mode;
    }

    public ExecutorMetaData(ExecutorEnum executor, PipelineMode cap, PipelineMode mode) {
        this.executor = executor;
        this.pipelineCap = cap;
        this.executionMode = mode;
    }

    public ExecutorEnum getExecutor() {
        return executor;
    }

    public PipelineMode getExecutionMode() {
        return executionMode;
    }

    public PipelineMode getPipelineCap() {
        return pipelineCap;
    }

    public void setProducer(int producer) {
        this.producer = new CountDownLatch(producer);
    }

    public CountDownLatch getProducer() {
        return producer;
    }
}
