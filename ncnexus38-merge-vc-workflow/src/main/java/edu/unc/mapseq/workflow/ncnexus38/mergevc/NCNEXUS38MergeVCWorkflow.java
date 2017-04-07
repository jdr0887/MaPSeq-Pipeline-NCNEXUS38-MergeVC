package edu.unc.mapseq.workflow.ncnexus38.mergevc;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobBuilder;
import org.renci.jlrm.condor.CondorJobEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.commons.ncnexus38.mergevc.RegisterToIRODSRunnable;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.Attribute;
import edu.unc.mapseq.dao.model.FileData;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.dao.model.WorkflowRun;
import edu.unc.mapseq.dao.model.WorkflowRunAttempt;
import edu.unc.mapseq.module.core.RemoveCLI;
import edu.unc.mapseq.module.core.ZipCLI;
import edu.unc.mapseq.module.sequencing.SureSelectTriggerSplitterCLI;
import edu.unc.mapseq.module.sequencing.filter.FilterVariantCLI;
import edu.unc.mapseq.module.sequencing.freebayes.FreeBayesCLI;
import edu.unc.mapseq.module.sequencing.gatk3.GATKVariantAnnotatorCLI;
import edu.unc.mapseq.module.sequencing.picard.PicardAddOrReplaceReadGroupsCLI;
import edu.unc.mapseq.module.sequencing.picard.PicardMarkDuplicatesCLI;
import edu.unc.mapseq.module.sequencing.picard.PicardMergeSAMCLI;
import edu.unc.mapseq.module.sequencing.picard.PicardSortOrderType;
import edu.unc.mapseq.module.sequencing.picard2.PicardCollectHsMetricsCLI;
import edu.unc.mapseq.module.sequencing.picard2.PicardMarkDuplicates;
import edu.unc.mapseq.module.sequencing.picard2.PicardSortVCFCLI;
import edu.unc.mapseq.module.sequencing.samtools.SAMToolsDepthCLI;
import edu.unc.mapseq.module.sequencing.samtools.SAMToolsFlagstatCLI;
import edu.unc.mapseq.module.sequencing.samtools.SAMToolsIndexCLI;
import edu.unc.mapseq.module.sequencing.vcflib.MergeVCFCLI;
import edu.unc.mapseq.module.sequencing.vcflib.SortAndRemoveDuplicatesCLI;
import edu.unc.mapseq.module.sequencing.vcflib.VCFFilterCLI;
import edu.unc.mapseq.workflow.WorkflowException;
import edu.unc.mapseq.workflow.core.WorkflowJobFactory;
import edu.unc.mapseq.workflow.core.WorkflowUtil;
import edu.unc.mapseq.workflow.sequencing.AbstractSequencingWorkflow;
import edu.unc.mapseq.workflow.sequencing.SequencingWorkflowJobFactory;
import edu.unc.mapseq.workflow.sequencing.SequencingWorkflowUtil;

public class NCNEXUS38MergeVCWorkflow extends AbstractSequencingWorkflow {

    private static final Logger logger = LoggerFactory.getLogger(NCNEXUS38MergeVCWorkflow.class);

    public NCNEXUS38MergeVCWorkflow() {
        super();
    }

    @Override
    public Graph<CondorJob, CondorJobEdge> createGraph() throws WorkflowException {
        logger.debug("ENTERING createGraph()");

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(CondorJobEdge.class);

        int count = 0;

        Set<Sample> sampleSet = SequencingWorkflowUtil.getAggregatedSamples(getWorkflowBeanService().getMaPSeqDAOBeanService(),
                getWorkflowRunAttempt());
        logger.info("sampleSet.size(): {}", sampleSet.size());

        String siteName = getWorkflowBeanService().getAttributes().get("siteName");
        String subjectMergeHome = getWorkflowBeanService().getAttributes().get("subjectMergeHome");

        String referenceSequence = getWorkflowBeanService().getAttributes().get("referenceSequence");
        String baitIntervalList = getWorkflowBeanService().getAttributes().get("baitIntervalList");
        String targetIntervalList = getWorkflowBeanService().getAttributes().get("targetIntervalList");
        String bed = getWorkflowBeanService().getAttributes().get("bed");
        String par1Coordinate = getWorkflowBeanService().getAttributes().get("par1Coordinate");
        String par2Coordinate = getWorkflowBeanService().getAttributes().get("par2Coordinate");
        String numberOfFreeBayesSubsets = getWorkflowBeanService().getAttributes().get("numberOfFreeBayesSubsets");
        String gender = getWorkflowBeanService().getAttributes().get("gender");
        String sselProbe = getWorkflowBeanService().getAttributes().get("sselProbe");
        String icSNPIntervalList = getWorkflowBeanService().getAttributes().get("icSNPIntervalList");

        WorkflowRunAttempt attempt = getWorkflowRunAttempt();
        WorkflowRun workflowRun = attempt.getWorkflowRun();

        try {

            Set<String> subjectNameSet = new HashSet<String>();
            for (Sample sample : sampleSet) {

                if ("Undetermined".equals(sample.getBarcode())) {
                    continue;
                }

                logger.info(sample.toString());

                Set<Attribute> sampleAttributes = sample.getAttributes();
                if (sampleAttributes != null && !sampleAttributes.isEmpty()) {
                    for (Attribute attribute : sampleAttributes) {
                        if (attribute.getName().equals("subjectName")) {
                            subjectNameSet.add(attribute.getValue());
                            break;
                        }
                    }
                }
            }

            Set<String> synchronizedSubjectNameSet = Collections.synchronizedSet(subjectNameSet);

            if (synchronizedSubjectNameSet.isEmpty()) {
                throw new WorkflowException("subjectNameSet is empty");
            }

            if (synchronizedSubjectNameSet.size() > 1) {
                throw new WorkflowException("multiple subjectName values across samples");
            }

            String subjectName = synchronizedSubjectNameSet.iterator().next();

            if (StringUtils.isEmpty(subjectName)) {
                throw new WorkflowException("empty subjectName");
            }

            Set<Attribute> workflowRunAttributeSet = workflowRun.getAttributes();

            if (CollectionUtils.isNotEmpty(workflowRunAttributeSet)) {
                Optional<Attribute> foundAttribute = workflowRunAttributeSet.stream().filter(a -> "sselProbe".equals(a.getName()))
                        .findFirst();
                if (foundAttribute.isPresent()) {
                    sselProbe = foundAttribute.get().getValue();
                }
                foundAttribute = workflowRunAttributeSet.stream().filter(a -> "gender".equals(a.getName())).findFirst();
                if (foundAttribute.isPresent()) {
                    gender = foundAttribute.get().getValue();
                }
                foundAttribute = workflowRunAttributeSet.stream().filter(a -> "freeBayesJobCount".equals(a.getName())).findFirst();
                if (foundAttribute.isPresent()) {
                    numberOfFreeBayesSubsets = foundAttribute.get().getValue();
                }
            }

            switch (sselProbe) {
                case "5 + EGL":
                    baitIntervalList = "$NCNEXUS_RESOURCES_DIRECTORY/intervals/agilent_v5_egl_capture_region_pm_100.shortid.38.interval_list";
                    targetIntervalList = "$NCNEXUS_RESOURCES_DIRECTORY/intervals/agilent_v5_egl_capture_region_pm_100.shortid.38.interval_list";
                    bed = "$NCNEXUS_RESOURCES_DIRECTORY/intervals/agilent_v5_egl_capture_region_pm_100.shortid.38.bed";
                    break;
                case "6":
                    baitIntervalList = "$NCNEXUS_RESOURCES_DIRECTORY/intervals/agilent_v6_capture_region_pm_100.shortid.38.interval_list";
                    targetIntervalList = "$NCNEXUS_RESOURCES_DIRECTORY/intervals/agilent_v6_capture_region_pm_100.shortid.38.interval_list";
                    bed = "$NCNEXUS_RESOURCES_DIRECTORY/intervals/agilent_v6_capture_region_pm_100.shortid.38.bed";
                    break;
                case "5":
                default:
                    baitIntervalList = "$NCNEXUS_RESOURCES_DIRECTORY/intervals/agilent_v5_capture_region_pm_100.shortid.38.interval_list";
                    targetIntervalList = "$NCNEXUS_RESOURCES_DIRECTORY/intervals/agilent_v5_capture_region_pm_100.shortid.38.interval_list";
                    bed = "$NCNEXUS_RESOURCES_DIRECTORY/intervals/agilent_v5_capture_region_pm_100.shortid.38.bed";
                    break;
            }

            // project directories
            File subjectDirectory = new File(subjectMergeHome, subjectName);
            subjectDirectory.mkdirs();
            logger.info("subjectDirectory.getAbsolutePath(): {}", subjectDirectory.getAbsolutePath());

            List<File> bamFileList = new ArrayList<File>();

            Workflow baselineWorkflow = null;
            try {
                List<Workflow> workflowList = getWorkflowBeanService().getMaPSeqDAOBeanService().getWorkflowDAO()
                        .findByName("NCNEXUS38Alignment");
                if (CollectionUtils.isEmpty(workflowList)) {
                    throw new WorkflowException("Could not find NCNEXUS38Alignment workflow");
                }
                baselineWorkflow = workflowList.get(0);
            } catch (MaPSeqDAOException e1) {
                e1.printStackTrace();
            }

            for (Sample sample : sampleSet) {

                if ("Undetermined".equals(sample.getBarcode())) {
                    continue;
                }

                Set<FileData> fileDataSet = sample.getFileDatas();

                File bamFile = WorkflowUtil.findFileByJobAndMimeTypeAndWorkflowId(getWorkflowBeanService().getMaPSeqDAOBeanService(),
                        fileDataSet, PicardMarkDuplicates.class, MimeType.APPLICATION_BAM, baselineWorkflow.getId());

                // 2nd attempt to find bam file
                if (bamFile == null) {
                    for (FileData fileData : fileDataSet) {
                        if (fileData.getName().endsWith(".md.bam")) {
                            bamFile = new File(fileData.getPath(), fileData.getName());
                            break;
                        }
                    }
                }

                // 3rd attempt to find bam file
                if (bamFile == null) {

                    File baselineOutputDirectory = SequencingWorkflowUtil.createOutputDirectory(sample, baselineWorkflow);
                    if (baselineOutputDirectory.exists()) {
                        for (File f : baselineOutputDirectory.listFiles()) {
                            if (f.getName().endsWith(".md.bam")) {
                                bamFile = f;
                                break;
                            }
                        }
                    }
                }

                if (bamFile == null) {
                    throw new WorkflowException("No BAM file found");
                }

                bamFileList.add(bamFile);

            }

            // new job
            CondorJobBuilder builder = WorkflowJobFactory.createJob(++count, PicardMergeSAMCLI.class, attempt.getId()).siteName(siteName);
            File mergeBAMFilesOut = new File(subjectDirectory, String.format("%s.merged.bam", subjectName));
            builder.addArgument(PicardMergeSAMCLI.SORTORDER, "unsorted").addArgument(PicardMergeSAMCLI.OUTPUT,
                    mergeBAMFilesOut.getAbsolutePath());
            for (File f : bamFileList) {
                logger.info("Using file: {}", f.getAbsolutePath());
                builder.addArgument(PicardMergeSAMCLI.INPUT, f.getAbsolutePath());
            }
            CondorJob mergeBAMFilesJob = builder.build();
            logger.info(mergeBAMFilesJob.toString());
            graph.addVertex(mergeBAMFilesJob);

            // new job
            builder = WorkflowJobFactory.createJob(++count, PicardAddOrReplaceReadGroupsCLI.class, attempt.getId()).siteName(siteName);
            File picardAddOrReplaceReadGroupsOut = new File(subjectDirectory, mergeBAMFilesOut.getName().replace(".bam", ".rg.bam"));
            builder.addArgument(PicardAddOrReplaceReadGroupsCLI.INPUT, mergeBAMFilesOut.getAbsolutePath())
                    .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPCENTERNAME, "UNC")
                    .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPID, subjectName)
                    .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPLIBRARY, subjectName)
                    .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPPLATFORM, "Illumina HiSeq 2000")
                    .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPPLATFORMUNIT, subjectName)
                    .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPSAMPLENAME, subjectName)
                    .addArgument(PicardAddOrReplaceReadGroupsCLI.SORTORDER, PicardSortOrderType.COORDINATE.toString().toLowerCase())
                    .addArgument(PicardAddOrReplaceReadGroupsCLI.OUTPUT, picardAddOrReplaceReadGroupsOut.getAbsolutePath());
            CondorJob picardAddOrReplaceReadGroupsJob = builder.build();
            logger.info(picardAddOrReplaceReadGroupsJob.toString());
            graph.addVertex(picardAddOrReplaceReadGroupsJob);
            graph.addEdge(mergeBAMFilesJob, picardAddOrReplaceReadGroupsJob);

            // new job
            builder = WorkflowJobFactory.createJob(++count, PicardMarkDuplicatesCLI.class, attempt.getId()).siteName(siteName);
            File picardMarkDuplicatesOutput = new File(subjectDirectory,
                    picardAddOrReplaceReadGroupsOut.getName().replace(".bam", ".deduped.bam"));
            File picardMarkDuplicatesMetrics = new File(subjectDirectory, picardMarkDuplicatesOutput.getName().replace(".bam", ".metrics"));
            builder.addArgument(PicardMarkDuplicatesCLI.INPUT, picardAddOrReplaceReadGroupsOut.getAbsolutePath())
                    .addArgument(PicardMarkDuplicatesCLI.OUTPUT, picardMarkDuplicatesOutput.getAbsolutePath())
                    .addArgument(PicardMarkDuplicatesCLI.METRICSFILE, picardMarkDuplicatesMetrics.getAbsolutePath());
            CondorJob picardMarkDuplicatesJob = builder.build();
            logger.info(picardMarkDuplicatesJob.toString());
            graph.addVertex(picardMarkDuplicatesJob);
            graph.addEdge(picardAddOrReplaceReadGroupsJob, picardMarkDuplicatesJob);

            // new job
            builder = WorkflowJobFactory.createJob(++count, SAMToolsIndexCLI.class, attempt.getId()).siteName(siteName);
            File samtoolsIndexOutput = new File(subjectDirectory, picardMarkDuplicatesOutput.getName().replace(".bam", ".bai"));
            builder.addArgument(SAMToolsIndexCLI.INPUT, picardMarkDuplicatesOutput.getAbsolutePath()).addArgument(SAMToolsIndexCLI.OUTPUT,
                    samtoolsIndexOutput.getAbsolutePath());
            CondorJob samtoolsIndexJob = builder.build();
            logger.info(samtoolsIndexJob.toString());
            graph.addVertex(samtoolsIndexJob);
            graph.addEdge(picardMarkDuplicatesJob, samtoolsIndexJob);

            // new job
            builder = WorkflowJobFactory.createJob(++count, SAMToolsFlagstatCLI.class, attempt.getId()).siteName(siteName);
            File samtoolsFlagstatOutput = new File(subjectDirectory, picardMarkDuplicatesOutput.getName().replace(".bam", ".flagstat"));
            builder.addArgument(SAMToolsFlagstatCLI.INPUT, picardMarkDuplicatesOutput.getAbsolutePath())
                    .addArgument(SAMToolsFlagstatCLI.OUTPUT, samtoolsFlagstatOutput.getAbsolutePath());
            CondorJob samtoolsFlagstatJob = builder.build();
            logger.info(samtoolsFlagstatJob.toString());
            graph.addVertex(samtoolsFlagstatJob);
            graph.addEdge(samtoolsIndexJob, samtoolsFlagstatJob);

            // new job
            builder = SequencingWorkflowJobFactory.createJob(++count, ZipCLI.class, attempt.getId()).siteName(siteName);
            File zipOut = new File(subjectDirectory, picardMarkDuplicatesOutput.getName().replace(".bam", ".zip"));
            builder.addArgument(ZipCLI.WORKDIR, subjectDirectory.getAbsolutePath())
                    .addArgument(ZipCLI.ENTRY, picardMarkDuplicatesOutput.getAbsolutePath())
                    .addArgument(ZipCLI.ENTRY, samtoolsIndexOutput.getAbsolutePath()).addArgument(ZipCLI.OUTPUT, zipOut.getAbsolutePath());
            CondorJob zipJob = builder.build();
            logger.info(zipJob.toString());
            graph.addVertex(zipJob);
            graph.addEdge(samtoolsIndexJob, zipJob);

            // new job
            builder = SequencingWorkflowJobFactory.createJob(++count, SAMToolsDepthCLI.class, attempt.getId()).siteName(siteName);
            File mergeBAMFilesDepthOut = new File(subjectDirectory, picardMarkDuplicatesOutput.getName().replace(".bam", ".depth.txt"));
            builder.addArgument(SAMToolsDepthCLI.BAM, picardMarkDuplicatesOutput.getAbsolutePath())
                    .addArgument(SAMToolsIndexCLI.OUTPUT, mergeBAMFilesDepthOut.getAbsolutePath()).addArgument(SAMToolsDepthCLI.BED, bed)
                    .addArgument(SAMToolsDepthCLI.BASEQUALITY, 20).addArgument(SAMToolsDepthCLI.MAPPINGQUALITY, 20);
            CondorJob samtoolsDepthJob = builder.build();
            logger.info(samtoolsDepthJob.toString());
            graph.addVertex(samtoolsDepthJob);
            graph.addEdge(samtoolsIndexJob, samtoolsDepthJob);

            // new job
            builder = SequencingWorkflowJobFactory.createJob(++count, PicardCollectHsMetricsCLI.class, attempt.getId()).siteName(siteName);
            File picardCollectHsMetricsFile = new File(subjectDirectory,
                    picardMarkDuplicatesOutput.getName().replace(".bam", ".hs.metrics"));
            builder.addArgument(PicardCollectHsMetricsCLI.INPUT, picardMarkDuplicatesOutput.getAbsolutePath())
                    .addArgument(PicardCollectHsMetricsCLI.OUTPUT, picardCollectHsMetricsFile.getAbsolutePath())
                    .addArgument(PicardCollectHsMetricsCLI.REFERENCESEQUENCE, referenceSequence)
                    .addArgument(PicardCollectHsMetricsCLI.BAITINTERVALS, baitIntervalList)
                    .addArgument(PicardCollectHsMetricsCLI.TARGETINTERVALS, targetIntervalList);
            CondorJob picardCollectHsMetricsJob = builder.build();
            logger.info(picardCollectHsMetricsJob.toString());
            graph.addVertex(picardCollectHsMetricsJob);
            graph.addEdge(samtoolsIndexJob, picardCollectHsMetricsJob);

            // new job
            builder = SequencingWorkflowJobFactory.createJob(++count, SureSelectTriggerSplitterCLI.class, attempt.getId())
                    .siteName(siteName);
            builder.addArgument(SureSelectTriggerSplitterCLI.GENDER, gender).addArgument(SureSelectTriggerSplitterCLI.BED, bed)
                    .addArgument(SureSelectTriggerSplitterCLI.WORKDIRECTORY, subjectDirectory.getAbsolutePath())
                    .addArgument(SureSelectTriggerSplitterCLI.SUBJECTNAME, subjectName)
                    .addArgument(SureSelectTriggerSplitterCLI.NUMBEROFSUBSETS, numberOfFreeBayesSubsets)
                    .addArgument(SureSelectTriggerSplitterCLI.PAR1COORDINATE, par1Coordinate)
                    .addArgument(SureSelectTriggerSplitterCLI.PAR2COORDINATE, par2Coordinate)
                    .addArgument(SureSelectTriggerSplitterCLI.OUTPUTPREFIX, String.format("%s_Trg", subjectName));
            CondorJob sureSelectTriggerSplitterJob = builder.build();
            logger.info(sureSelectTriggerSplitterJob.toString());
            graph.addVertex(sureSelectTriggerSplitterJob);
            graph.addEdge(samtoolsIndexJob, sureSelectTriggerSplitterJob);

            List<CondorJob> mergeVCFParentJobs = new ArrayList<CondorJob>();

            File ploidyFile = new File(subjectDirectory, String.format("%s_Trg.ploidy.bed", subjectName));
            for (int i = 0; i < Integer.valueOf(numberOfFreeBayesSubsets); i++) {

                // new job
                builder = SequencingWorkflowJobFactory.createJob(++count, FreeBayesCLI.class, attempt.getId()).siteName(siteName);
                File freeBayesOutput = new File(subjectDirectory, String.format("%s_Trg.set%d.vcf", subjectName, i + 1));
                File targetFile = new File(subjectDirectory, String.format("%s_Trg.interval.set%d.bed", subjectName, i + 1));
                builder.addArgument(FreeBayesCLI.GENOTYPEQUALITIES).addArgument(FreeBayesCLI.REPORTMONOMORPHIC)
                        .addArgument(FreeBayesCLI.BAM, picardMarkDuplicatesOutput.getAbsolutePath())
                        .addArgument(FreeBayesCLI.VCF, freeBayesOutput.getAbsolutePath())
                        .addArgument(FreeBayesCLI.FASTAREFERENCE, referenceSequence)
                        .addArgument(FreeBayesCLI.TARGETS, targetFile.getAbsolutePath())
                        .addArgument(FreeBayesCLI.COPYNUMBERMAP, ploidyFile.getAbsolutePath());
                CondorJob freeBayesJob = builder.build();
                logger.info(freeBayesJob.toString());
                graph.addVertex(freeBayesJob);
                graph.addEdge(sureSelectTriggerSplitterJob, freeBayesJob);
                mergeVCFParentJobs.add(freeBayesJob);

            }

            // new job
            builder = SequencingWorkflowJobFactory.createJob(++count, MergeVCFCLI.class, attempt.getId()).siteName(siteName);
            File mergeVCFOutput = new File(subjectDirectory, picardMarkDuplicatesOutput.getName().replace(".bam", ".vcf"));
            builder.addArgument(MergeVCFCLI.INPUT, String.format("%s_Trg.*.vcf", subjectName))
                    .addArgument(MergeVCFCLI.WORKDIRECTORY, subjectDirectory.getAbsolutePath())
                    .addArgument(MergeVCFCLI.OUTPUT, mergeVCFOutput.getAbsolutePath());
            CondorJob mergeVCFJob = builder.build();
            logger.info(mergeVCFJob.toString());
            graph.addVertex(mergeVCFJob);
            for (CondorJob job : mergeVCFParentJobs) {
                graph.addEdge(job, mergeVCFJob);
            }

            // new job
            builder = SequencingWorkflowJobFactory.createJob(++count, VCFFilterCLI.class, attempt.getId()).siteName(siteName);
            File vcfFilterOutput = new File(subjectDirectory, mergeVCFOutput.getName().replace(".vcf", ".filtered.vcf"));
            // need to do character conversion since vcffilter uses spaces & commons cli does not work will with spaces
            builder.addArgument(VCFFilterCLI.INPUT, mergeVCFOutput.getAbsolutePath())
                    .addArgument(VCFFilterCLI.OUTPUT, vcfFilterOutput.getAbsolutePath()).addArgument(VCFFilterCLI.INFOFILTER, "QUAL_>_10")
                    .addArgument(VCFFilterCLI.INFOFILTER, "DP_>_5");
            CondorJob vcfFilterJob = builder.build();
            logger.info(vcfFilterJob.toString());
            graph.addVertex(vcfFilterJob);
            graph.addEdge(mergeVCFJob, vcfFilterJob);

            // new job
            builder = SequencingWorkflowJobFactory.createJob(++count, SortAndRemoveDuplicatesCLI.class, attempt.getId()).siteName(siteName);
            File sortAndRemoveDuplicatesOutput = new File(subjectDirectory, vcfFilterOutput.getName().replace(".vcf", ".srd.vcf"));
            builder.addArgument(SortAndRemoveDuplicatesCLI.INPUT, vcfFilterOutput.getAbsolutePath())
                    .addArgument(SortAndRemoveDuplicatesCLI.OUTPUT, sortAndRemoveDuplicatesOutput.getAbsolutePath());
            CondorJob sortAndRemoveDuplicatesJob = builder.build();
            logger.info(sortAndRemoveDuplicatesJob.toString());
            graph.addVertex(sortAndRemoveDuplicatesJob);
            graph.addEdge(vcfFilterJob, sortAndRemoveDuplicatesJob);

            // new job
            builder = SequencingWorkflowJobFactory.createJob(++count, PicardSortVCFCLI.class, attempt.getId()).siteName(siteName);
            File picardSortVCFOutput = new File(subjectDirectory, sortAndRemoveDuplicatesOutput.getName().replace(".vcf", ".ps.vcf"));
            builder.addArgument(PicardSortVCFCLI.INPUT, sortAndRemoveDuplicatesOutput.getAbsolutePath())
                    .addArgument(PicardSortVCFCLI.OUTPUT, picardSortVCFOutput.getAbsolutePath());
            CondorJob picardSortVCFJob = builder.build();
            logger.info(picardSortVCFJob.toString());
            graph.addVertex(picardSortVCFJob);
            graph.addEdge(sortAndRemoveDuplicatesJob, picardSortVCFJob);

            // new job
            builder = SequencingWorkflowJobFactory.createJob(++count, GATKVariantAnnotatorCLI.class, attempt.getId()).siteName(siteName);
            File gatkVariantAnnotatorOutput = new File(subjectDirectory, picardSortVCFOutput.getName().replace(".vcf", ".va.vcf"));
            builder.addArgument(GATKVariantAnnotatorCLI.VCF, picardSortVCFOutput.getAbsolutePath())
                    .addArgument(GATKVariantAnnotatorCLI.ANNOTATION, "FisherStrand")
                    .addArgument(GATKVariantAnnotatorCLI.ANNOTATION, "QualByDepth")
                    .addArgument(GATKVariantAnnotatorCLI.ANNOTATION, "ReadPosRankSumTest")
                    .addArgument(GATKVariantAnnotatorCLI.ANNOTATION, "DepthPerAlleleBySample")
                    .addArgument(GATKVariantAnnotatorCLI.ANNOTATION, "HomopolymerRun")
                    .addArgument(GATKVariantAnnotatorCLI.ANNOTATION, "SpanningDeletions")
                    .addArgument(GATKVariantAnnotatorCLI.BAM, picardMarkDuplicatesOutput.getAbsolutePath())
                    .addArgument(GATKVariantAnnotatorCLI.REFERENCESEQUENCE, referenceSequence)
                    .addArgument(GATKVariantAnnotatorCLI.OUT, gatkVariantAnnotatorOutput.getAbsolutePath());
            CondorJob gatkVCFJob = builder.build();
            logger.info(gatkVCFJob.toString());
            graph.addVertex(gatkVCFJob);
            graph.addEdge(picardSortVCFJob, gatkVCFJob);

            // new job
            builder = SequencingWorkflowJobFactory.createJob(++count, FilterVariantCLI.class, attempt.getId()).siteName(siteName);
            File filterVariantOutput = new File(subjectDirectory, gatkVariantAnnotatorOutput.getName().replace(".vcf", ".ic.vcf"));
            builder.addArgument(FilterVariantCLI.INTERVALLIST, icSNPIntervalList)
                    .addArgument(FilterVariantCLI.INPUT, gatkVariantAnnotatorOutput.getAbsolutePath())
                    .addArgument(FilterVariantCLI.OUTPUT, filterVariantOutput.getAbsolutePath());
            CondorJob filterVariantJob = builder.build();
            logger.info(filterVariantJob.toString());
            graph.addVertex(filterVariantJob);
            graph.addEdge(gatkVCFJob, filterVariantJob);

            // new job
            builder = WorkflowJobFactory.createJob(++count, RemoveCLI.class, attempt.getId()).siteName(siteName);
            builder.addArgument(RemoveCLI.FILE, mergeBAMFilesOut.getAbsolutePath())
                    .addArgument(RemoveCLI.FILE, vcfFilterOutput.getAbsolutePath())
                    .addArgument(RemoveCLI.FILE, picardAddOrReplaceReadGroupsOut.getAbsolutePath())
                    .addArgument(RemoveCLI.FILE, mergeVCFOutput.getAbsolutePath())
                    .addArgument(RemoveCLI.FILE, sortAndRemoveDuplicatesOutput.getAbsolutePath())
                    .addArgument(RemoveCLI.FILE, sortAndRemoveDuplicatesOutput.getAbsolutePath().replace(".vcf", ".vcf.idx"))
                    .addArgument(RemoveCLI.FILE, ploidyFile.getAbsolutePath());
            for (int i = 0; i < Integer.valueOf(numberOfFreeBayesSubsets); i++) {
                File freeBayesOutput = new File(subjectDirectory, String.format("%s_Trg.set%d.vcf", subjectName, i + 1));
                File targetFile = new File(subjectDirectory, String.format("%s_Trg.interval.set%d.bed", subjectName, i + 1));
                builder.addArgument(RemoveCLI.FILE, freeBayesOutput.getAbsolutePath()).addArgument(RemoveCLI.FILE,
                        targetFile.getAbsolutePath());
            }
            CondorJob removeJob = builder.build();
            logger.info(removeJob.toString());
            graph.addVertex(removeJob);
            graph.addEdge(filterVariantJob, removeJob);

        } catch (Exception e) {
            throw new WorkflowException(e);
        }

        return graph;
    }

    @Override
    public void postRun() throws WorkflowException {
        logger.info("ENTERING postRun()");
        super.postRun();
        try {
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            RegisterToIRODSRunnable registerToIRODSRunnable = new RegisterToIRODSRunnable(
                    getWorkflowBeanService().getMaPSeqDAOBeanService(), getWorkflowRunAttempt());
            executorService.submit(registerToIRODSRunnable);
            executorService.shutdown();
            executorService.awaitTermination(1L, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            throw new WorkflowException(e);
        }
    }

}
