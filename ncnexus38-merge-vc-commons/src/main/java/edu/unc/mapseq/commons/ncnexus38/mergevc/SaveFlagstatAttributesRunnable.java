package edu.unc.mapseq.commons.ncnexus38.mergevc;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.AttributeDAO;
import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.SampleDAO;
import edu.unc.mapseq.dao.WorkflowDAO;
import edu.unc.mapseq.dao.model.Attribute;
import edu.unc.mapseq.dao.model.FileData;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.workflow.sequencing.SequencingWorkflowUtil;

public class SaveFlagstatAttributesRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(SaveFlagstatAttributesRunnable.class);

    private Long sampleId;

    private Long flowcellId;

    private String subjectMergeHome;

    private MaPSeqDAOBeanService mapseqDAOBeanService;

    public SaveFlagstatAttributesRunnable(MaPSeqDAOBeanService mapseqDAOBeanService) {
        super();
        this.mapseqDAOBeanService = mapseqDAOBeanService;
    }

    @Override
    public void run() {
        logger.info("ENTERING run()");

        try {
            Set<Sample> sampleSet = new HashSet<Sample>();

            SampleDAO sampleDAO = mapseqDAOBeanService.getSampleDAO();
            AttributeDAO attributeDAO = mapseqDAOBeanService.getAttributeDAO();

            if (flowcellId != null) {
                sampleSet.addAll(sampleDAO.findByFlowcellId(flowcellId));
            }

            if (sampleId != null) {
                Sample sample = sampleDAO.findById(sampleId);
                if (sample == null) {
                    logger.error("Sample was not found");
                    return;
                }
                sampleSet.add(sample);
            }

            Workflow workflow = null;
            WorkflowDAO workflowDAO = mapseqDAOBeanService.getWorkflowDAO();
            List<Workflow> workflowList = workflowDAO.findByName("NCNEXUSBaseline");

            if (CollectionUtils.isNotEmpty(workflowList)) {
                workflow = workflowList.get(0);
            }

            if (workflow == null) {
                logger.error("NCNEXUSBaseline workflow not found");
                return;
            }

            for (Sample sample : sampleSet) {

                logger.info(sample.toString());

                String subjectName = "";
                Set<Attribute> sampleAttributes = sample.getAttributes();
                if (sampleAttributes != null && !sampleAttributes.isEmpty()) {
                    for (Attribute attribute : sampleAttributes) {
                        if (attribute.getName().equals("subjectName")) {
                            subjectName = attribute.getValue();
                            break;
                        }
                    }
                }

                if (StringUtils.isEmpty(subjectName)) {
                    logger.error("subjectName is empty");
                    return;
                }

                // project directories
                File subjectDirectory = new File(subjectMergeHome, subjectName);
                subjectDirectory.mkdirs();
                logger.info("subjectDirectory.getAbsolutePath(): {}", subjectDirectory.getAbsolutePath());

                File outputDirectory = SequencingWorkflowUtil.createOutputDirectory(sample, workflow);

                Set<Attribute> attributeSet = sample.getAttributes();
                Set<FileData> sampleFileDataSet = sample.getFileDatas();

                File flagstatFile = null;

                for (FileData fileData : sampleFileDataSet) {
                    if (MimeType.TEXT_STAT_SUMMARY.equals(fileData.getMimeType()) && fileData.getName().endsWith(".flagstat")) {
                        flagstatFile = new File(fileData.getPath(), fileData.getName());
                        break;
                    }
                }

                if (flagstatFile == null) {
                    logger.error("flagstat file to process was not found...checking FS");
                    if (outputDirectory.exists()) {
                        File[] files = outputDirectory.listFiles();
                        if (files != null && files.length > 0) {
                            for (File file : files) {
                                if (file.getName().endsWith("samtools.flagstat")) {
                                    flagstatFile = file;
                                    break;
                                }
                            }
                        }
                    }
                }

                if (flagstatFile == null) {
                    logger.error("flagstat file to process was still not found");
                    continue;
                }

                logger.info("flagstat file is: {}", flagstatFile.getAbsolutePath());

                List<String> lines = FileUtils.readLines(flagstatFile);

                Set<String> attributeNameSet = new HashSet<String>();
                attributeSet.forEach(a -> attributeNameSet.add(a.getName()));

                Set<String> synchSet = Collections.synchronizedSet(attributeNameSet);

                if (lines != null) {

                    for (String line : lines) {

                        if (line.contains("in total")) {
                            String value = line.substring(0, line.indexOf(" ")).trim();
                            if (synchSet.contains("SAMToolsFlagstat.totalPassedReads")) {
                                for (Attribute attribute : attributeSet) {
                                    if (attribute.getName().equals("SAMToolsFlagstat.totalPassedReads")) {
                                        attribute.setValue(value);
                                        try {
                                            attributeDAO.save(attribute);
                                        } catch (MaPSeqDAOException e) {
                                            logger.error("MaPSeqDAOException", e);
                                        }
                                        break;
                                    }
                                }
                            } else {
                                attributeSet.add(new Attribute("SAMToolsFlagstat.totalPassedReads", value));
                            }
                        }

                        if (line.contains("mapped (")) {
                            Pattern pattern = Pattern.compile("^.+\\((.+)\\)");
                            Matcher matcher = pattern.matcher(line);
                            if (matcher.matches()) {
                                String value = matcher.group(1);
                                value = value.substring(0, value.indexOf("%")).trim();
                                if (StringUtils.isNotEmpty(value)) {
                                    if (synchSet.contains("SAMToolsFlagstat.aligned")) {
                                        for (Attribute attribute : attributeSet) {
                                            if (attribute.getName().equals("SAMToolsFlagstat.aligned")) {
                                                attribute.setValue(value);
                                                break;
                                            }
                                        }
                                    } else {
                                        attributeSet.add(new Attribute("SAMToolsFlagstat.aligned", value));
                                    }
                                }
                            }
                        }

                        if (line.contains("properly paired (")) {
                            Pattern pattern = Pattern.compile("^.+\\((.+)\\)");
                            Matcher matcher = pattern.matcher(line);
                            if (matcher.matches()) {
                                String value = matcher.group(1);
                                value = value.substring(0, value.indexOf("%"));
                                if (StringUtils.isNotEmpty(value)) {
                                    if (synchSet.contains("SAMToolsFlagstat.paired")) {
                                        for (Attribute attribute : attributeSet) {
                                            if (attribute.getName().equals("SAMToolsFlagstat.paired")) {
                                                attribute.setValue(value);
                                                break;
                                            }
                                        }
                                    } else {
                                        attributeSet.add(new Attribute("SAMToolsFlagstat.paired", value));
                                    }
                                }
                            }
                        }
                    }

                }

                sample.setAttributes(attributeSet);
                sampleDAO.save(sample);

            }
        } catch (MaPSeqDAOException | IOException e) {
            e.printStackTrace();
        }
    }

    public Long getSampleId() {
        return sampleId;
    }

    public void setSampleId(Long sampleId) {
        this.sampleId = sampleId;
    }

    public Long getFlowcellId() {
        return flowcellId;
    }

    public void setFlowcellId(Long flowcellId) {
        this.flowcellId = flowcellId;
    }

    public String getSubjectMergeHome() {
        return subjectMergeHome;
    }

    public void setSubjectMergeHome(String subjectMergeHome) {
        this.subjectMergeHome = subjectMergeHome;
    }

}
