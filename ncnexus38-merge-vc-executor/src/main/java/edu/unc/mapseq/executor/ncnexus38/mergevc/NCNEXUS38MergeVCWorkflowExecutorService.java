package edu.unc.mapseq.executor.ncnexus38.mergevc;

import java.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NCNEXUS38MergeVCWorkflowExecutorService {

    private static final Logger logger = LoggerFactory.getLogger(NCNEXUS38MergeVCWorkflowExecutorService.class);

    private final Timer mainTimer = new Timer();

    private NCNEXUS38MergeVCWorkflowExecutorTask task;

    private Long period = 5L;

    public NCNEXUS38MergeVCWorkflowExecutorService() {
        super();
    }

    public void start() throws Exception {
        logger.info("ENTERING start()");
        long delay = 1 * 60 * 1000;
        mainTimer.scheduleAtFixedRate(task, delay, period * 60 * 1000);
    }

    public void stop() throws Exception {
        logger.info("ENTERING stop()");
        mainTimer.purge();
        mainTimer.cancel();
    }

    public NCNEXUS38MergeVCWorkflowExecutorTask getTask() {
        return task;
    }

    public void setTask(NCNEXUS38MergeVCWorkflowExecutorTask task) {
        this.task = task;
    }

    public Long getPeriod() {
        return period;
    }

    public void setPeriod(Long period) {
        this.period = period;
    }

}
