package edu.unc.mapseq.commands.ncnexus38.mergevc;

import java.util.concurrent.Executors;

import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.Option;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.commons.ncnexus38.mergevc.SaveFlagstatAttributesRunnable;
import edu.unc.mapseq.dao.MaPSeqDAOBeanService;

@Command(scope = "ncnexus38-mergevc", name = "save-flagstat-attributes", description = "Save Flagstat Attributes")
@Service
public class SaveFlagstatAttributesAction implements Action {

    private static final Logger logger = LoggerFactory.getLogger(SaveFlagstatAttributesAction.class);

    @Option(name = "--sampleId", description = "sampleId", required = false, multiValued = false)
    private Long sampleId;

    @Option(name = "--flowcellId", description = "flowcellId", required = false, multiValued = false)
    private Long flowcellId;

    @Reference
    private MaPSeqDAOBeanService maPSeqDAOBeanService;

    public SaveFlagstatAttributesAction() {
        super();
    }

    @Override
    public Object execute() {
        logger.debug("ENTERING execute()");

        if (flowcellId == null && sampleId == null) {
            System.out.println("Both sampleId && flowcellId can't be null");
            return null;
        }

        SaveFlagstatAttributesRunnable runnable = new SaveFlagstatAttributesRunnable(maPSeqDAOBeanService);

        if (sampleId != null) {
            runnable.setSampleId(sampleId);
        }
        if (flowcellId != null) {
            runnable.setFlowcellId(flowcellId);
        }
        Executors.newSingleThreadExecutor().execute(runnable);

        return null;
    }

    public Long getFlowcellId() {
        return flowcellId;
    }

    public void setFlowcellId(Long flowcellId) {
        this.flowcellId = flowcellId;
    }

    public Long getSampleId() {
        return sampleId;
    }

    public void setSampleId(Long sampleId) {
        this.sampleId = sampleId;
    }

}
