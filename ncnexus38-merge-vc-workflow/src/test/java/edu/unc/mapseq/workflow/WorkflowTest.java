package edu.unc.mapseq.workflow;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.junit.Test;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobBuilder;
import org.renci.jlrm.condor.CondorJobEdge;
import org.renci.jlrm.condor.ext.CondorDOTExporter;

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
import edu.unc.mapseq.module.sequencing.picard2.PicardSortVCFCLI;
import edu.unc.mapseq.module.sequencing.samtools.SAMToolsDepthCLI;
import edu.unc.mapseq.module.sequencing.samtools.SAMToolsFlagstatCLI;
import edu.unc.mapseq.module.sequencing.samtools.SAMToolsIndexCLI;
import edu.unc.mapseq.module.sequencing.vcflib.MergeVCFCLI;
import edu.unc.mapseq.module.sequencing.vcflib.SortAndRemoveDuplicatesCLI;
import edu.unc.mapseq.module.sequencing.vcflib.VCFFilterCLI;

public class WorkflowTest {

    public WorkflowTest() throws WorkflowException {
        super();
    }

    @Test
    public void createGraph() throws WorkflowException {

        int count = 0;

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(CondorJobEdge.class);

        // // new job
        File mergeBAMFilesOut = new File(String.format("%s.merged.bam", "<subjectName>"));
        CondorJob mergeBAMFilesJob = new CondorJobBuilder().name(String.format("%s_%d", PicardMergeSAMCLI.class.getSimpleName(), ++count))
                .addArgument(PicardMergeSAMCLI.SORTORDER, "unsorted").addArgument(PicardMergeSAMCLI.OUTPUT, mergeBAMFilesOut.getName())
                .build();
        graph.addVertex(mergeBAMFilesJob);

        // new job
        File picardAddOrReplaceReadGroupsOut = new File(mergeBAMFilesOut.getName().replace(".bam", ".rg.bam"));
        CondorJob picardAddOrReplaceReadGroupsJob = new CondorJobBuilder()
                .name(String.format("%s_%d", PicardAddOrReplaceReadGroupsCLI.class.getSimpleName(), ++count))
                .addArgument(PicardAddOrReplaceReadGroupsCLI.INPUT, mergeBAMFilesOut.getName())
                .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPCENTERNAME, "UNC")
                .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPID, "<subjectName>")
                .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPLIBRARY, "<subjectName>")
                .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPPLATFORM, "Illumina HiSeq 2000")
                .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPPLATFORMUNIT, "<subjectName>")
                .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPSAMPLENAME, "<subjectName>")
                .addArgument(PicardAddOrReplaceReadGroupsCLI.SORTORDER, PicardSortOrderType.COORDINATE.toString().toLowerCase())
                .addArgument(PicardAddOrReplaceReadGroupsCLI.OUTPUT, picardAddOrReplaceReadGroupsOut.getName()).build();
        graph.addVertex(picardAddOrReplaceReadGroupsJob);
        graph.addEdge(mergeBAMFilesJob, picardAddOrReplaceReadGroupsJob);

        // new job
        File picardMarkDuplicatesOutput = new File(picardAddOrReplaceReadGroupsOut.getName().replace(".bam", ".deduped.bam"));
        File picardMarkDuplicatesMetrics = new File(picardMarkDuplicatesOutput.getName().replace(".bam", ".metrics"));
        CondorJob picardMarkDuplicatesJob = new CondorJobBuilder()
                .name(String.format("%s_%d", PicardMarkDuplicatesCLI.class.getSimpleName(), ++count))
                .addArgument(PicardMarkDuplicatesCLI.INPUT, picardAddOrReplaceReadGroupsOut.getName())
                .addArgument(PicardMarkDuplicatesCLI.OUTPUT, picardMarkDuplicatesOutput.getName())
                .addArgument(PicardMarkDuplicatesCLI.METRICSFILE, picardMarkDuplicatesMetrics.getName()).build();
        graph.addVertex(picardMarkDuplicatesJob);
        graph.addEdge(picardAddOrReplaceReadGroupsJob, picardMarkDuplicatesJob);

        // new job
        File samtoolsIndexOutput = new File(picardMarkDuplicatesOutput.getName().replace(".bam", ".bai"));
        CondorJob samtoolsIndexJob = new CondorJobBuilder().name(String.format("%s_%d", SAMToolsIndexCLI.class.getSimpleName(), ++count))
                .addArgument(SAMToolsIndexCLI.INPUT, picardMarkDuplicatesOutput.getName())
                .addArgument(SAMToolsIndexCLI.OUTPUT, samtoolsIndexOutput.getName()).build();
        graph.addVertex(samtoolsIndexJob);
        graph.addEdge(picardMarkDuplicatesJob, samtoolsIndexJob);

        // new job
        File samtoolsFlagstatOutput = new File(picardMarkDuplicatesOutput.getName().replace(".bam", ".flagstat"));
        CondorJob samtoolsFlagstatJob = new CondorJobBuilder()
                .name(String.format("%s_%d", SAMToolsFlagstatCLI.class.getSimpleName(), ++count))
                .addArgument(SAMToolsFlagstatCLI.INPUT, picardMarkDuplicatesOutput.getName())
                .addArgument(SAMToolsFlagstatCLI.OUTPUT, samtoolsFlagstatOutput.getName()).build();
        graph.addVertex(samtoolsFlagstatJob);
        graph.addEdge(samtoolsIndexJob, samtoolsFlagstatJob);

        // new job
        File zipOut = new File(picardMarkDuplicatesOutput.getName().replace(".bam", ".zip"));
        CondorJob zipJob = new CondorJobBuilder().name(String.format("%s_%d", ZipCLI.class.getSimpleName(), ++count))
                .addArgument(ZipCLI.WORKDIR, "<tmpdir>").addArgument(ZipCLI.ENTRY, picardMarkDuplicatesOutput.getName())
                .addArgument(ZipCLI.ENTRY, samtoolsIndexOutput.getName()).addArgument(ZipCLI.OUTPUT, zipOut.getName()).build();
        graph.addVertex(zipJob);
        graph.addEdge(samtoolsIndexJob, zipJob);

        // new job
        File mergeBAMFilesDepthOut = new File(picardMarkDuplicatesOutput.getName().replace(".bam", ".depth.txt"));
        CondorJob samtoolsDepthJob = new CondorJobBuilder().name(String.format("%s_%d", SAMToolsDepthCLI.class.getSimpleName(), ++count))
                .addArgument(SAMToolsDepthCLI.BAM, picardMarkDuplicatesOutput.getName())
                .addArgument(SAMToolsIndexCLI.OUTPUT, mergeBAMFilesDepthOut.getName()).addArgument(SAMToolsDepthCLI.BED, "<bed>")
                .addArgument(SAMToolsDepthCLI.BASEQUALITY, 20).addArgument(SAMToolsDepthCLI.MAPPINGQUALITY, 20).build();
        graph.addVertex(samtoolsDepthJob);
        graph.addEdge(samtoolsIndexJob, samtoolsDepthJob);

        // new job
        File picardCollectHsMetricsFile = new File(picardMarkDuplicatesOutput.getName().replace(".bam", ".hs.metrics"));
        CondorJob picardCollectHsMetricsJob = new CondorJobBuilder()
                .name(String.format("%s_%d", PicardCollectHsMetricsCLI.class.getSimpleName(), ++count))
                .addArgument(PicardCollectHsMetricsCLI.INPUT, picardMarkDuplicatesOutput.getName())
                .addArgument(PicardCollectHsMetricsCLI.OUTPUT, picardCollectHsMetricsFile.getName())
                .addArgument(PicardCollectHsMetricsCLI.REFERENCESEQUENCE, "<referenceSequence>")
                .addArgument(PicardCollectHsMetricsCLI.BAITINTERVALS, "<baitIntervalList>")
                .addArgument(PicardCollectHsMetricsCLI.TARGETINTERVALS, "<targetIntervalList>").build();
        graph.addVertex(picardCollectHsMetricsJob);
        graph.addEdge(samtoolsIndexJob, picardCollectHsMetricsJob);

        // new job
        CondorJob sureSelectTriggerSplitterJob = new CondorJobBuilder()
                .name(String.format("%s_%d", SureSelectTriggerSplitterCLI.class.getSimpleName(), ++count))
                .addArgument(SureSelectTriggerSplitterCLI.GENDER, "<gender>").addArgument(SureSelectTriggerSplitterCLI.BED, "<bed>")
                .addArgument(SureSelectTriggerSplitterCLI.WORKDIRECTORY, "<outputDirectory>")
                .addArgument(SureSelectTriggerSplitterCLI.SUBJECTNAME, "<subjectName>")
                .addArgument(SureSelectTriggerSplitterCLI.NUMBEROFSUBSETS, "<numberOfFreeBayesSubsets>")
                .addArgument(SureSelectTriggerSplitterCLI.PAR1COORDINATE, "<par1Coordinate>")
                .addArgument(SureSelectTriggerSplitterCLI.PAR2COORDINATE, "<par2Coordinate>")
                .addArgument(SureSelectTriggerSplitterCLI.OUTPUTPREFIX, String.format("%s_Trg", "<subjectName>")).build();
        graph.addVertex(sureSelectTriggerSplitterJob);
        graph.addEdge(samtoolsIndexJob, sureSelectTriggerSplitterJob);

        List<CondorJob> mergeVCFParentJobs = new ArrayList<CondorJob>();
        File ploidyFile = new File(String.format("%s_Trg.ploidy.bed", "<subjectName>"));

        for (int i = 0; i < 4; i++) {

            // new job
            File freeBayesOutput = new File(String.format("%s_Trg.set%d.vcf", "<subjectName>", i + 1));
            File targetFile = new File(String.format("%s_Trg.interval.set%d.bed", "<subjectName>", i + 1));
            CondorJob freeBayesJob = new CondorJobBuilder().name(String.format("%s_%d", FreeBayesCLI.class.getSimpleName(), ++count))
                    .addArgument(FreeBayesCLI.GENOTYPEQUALITIES).addArgument(FreeBayesCLI.REPORTMONOMORPHIC)
                    .addArgument(FreeBayesCLI.BAM, picardMarkDuplicatesOutput.getName())
                    .addArgument(FreeBayesCLI.VCF, freeBayesOutput.getName())
                    .addArgument(FreeBayesCLI.FASTAREFERENCE, "<referenceSequence>").addArgument(FreeBayesCLI.TARGETS, targetFile.getName())
                    .addArgument(FreeBayesCLI.COPYNUMBERMAP, ploidyFile.getName()).build();
            graph.addVertex(freeBayesJob);
            graph.addEdge(sureSelectTriggerSplitterJob, freeBayesJob);
            mergeVCFParentJobs.add(freeBayesJob);

        }

        // new job
        File mergeVCFOutput = new File(picardMarkDuplicatesOutput.getName().replace(".bam", ".vcf"));
        CondorJob mergeVCFJob = new CondorJobBuilder().name(String.format("%s_%d", MergeVCFCLI.class.getSimpleName(), ++count))
                .addArgument(MergeVCFCLI.INPUT, String.format("%s_Trg.*.vcf", "<subjectName>"))
                .addArgument(MergeVCFCLI.WORKDIRECTORY, "<outputDirectory>").addArgument(MergeVCFCLI.OUTPUT, mergeVCFOutput.getName())
                .build();
        graph.addVertex(mergeVCFJob);
        for (CondorJob job : mergeVCFParentJobs) {
            graph.addEdge(job, mergeVCFJob);
        }

        // new job
        File vcfFilterOutput = new File(mergeVCFOutput.getName().replace(".vcf", ".filtered.vcf"));
        CondorJob vcfFilterJob = new CondorJobBuilder().name(String.format("%s_%d", VCFFilterCLI.class.getSimpleName(), ++count))
                .addArgument(VCFFilterCLI.INPUT, mergeVCFOutput.getName()).addArgument(VCFFilterCLI.OUTPUT, vcfFilterOutput.getName())
                .addArgument(VCFFilterCLI.INFOFILTER, "QUAL_>_10").addArgument(VCFFilterCLI.INFOFILTER, "DP_>_5").build();
        graph.addVertex(vcfFilterJob);
        graph.addEdge(mergeVCFJob, vcfFilterJob);

        // new job
        File sortAndRemoveDuplicatesOutput = new File(vcfFilterOutput.getName().replace(".vcf", ".srd.vcf"));
        CondorJob sortAndRemoveDuplicatesJob = new CondorJobBuilder()
                .name(String.format("%s_%d", SortAndRemoveDuplicatesCLI.class.getSimpleName(), ++count))
                .addArgument(SortAndRemoveDuplicatesCLI.INPUT, vcfFilterOutput.getName())
                .addArgument(SortAndRemoveDuplicatesCLI.OUTPUT, sortAndRemoveDuplicatesOutput.getName()).build();
        graph.addVertex(sortAndRemoveDuplicatesJob);
        graph.addEdge(vcfFilterJob, sortAndRemoveDuplicatesJob);

        // new job
        File picardSortVCFOutput = new File(sortAndRemoveDuplicatesOutput.getName().replace(".vcf", ".ps.vcf"));
        CondorJob picardSortVCFJob = new CondorJobBuilder().name(String.format("%s_%d", PicardSortVCFCLI.class.getSimpleName(), ++count))
                .addArgument(PicardSortVCFCLI.INPUT, sortAndRemoveDuplicatesOutput.getName())
                .addArgument(PicardSortVCFCLI.OUTPUT, picardSortVCFOutput.getName()).build();
        graph.addVertex(picardSortVCFJob);
        graph.addEdge(sortAndRemoveDuplicatesJob, picardSortVCFJob);

        // new job
        File gatkVariantAnnotatorOutput = new File(picardSortVCFOutput.getName().replace(".vcf", ".va.vcf"));
        CondorJob gatkVCFJob = new CondorJobBuilder().name(String.format("%s_%d", GATKVariantAnnotatorCLI.class.getSimpleName(), ++count))
                .addArgument(GATKVariantAnnotatorCLI.VCF, picardSortVCFOutput.getName())
                .addArgument(GATKVariantAnnotatorCLI.ANNOTATION, "FisherStrand")
                .addArgument(GATKVariantAnnotatorCLI.ANNOTATION, "QualByDepth")
                .addArgument(GATKVariantAnnotatorCLI.ANNOTATION, "ReadPosRankSumTest")
                .addArgument(GATKVariantAnnotatorCLI.ANNOTATION, "DepthPerAlleleBySample")
                .addArgument(GATKVariantAnnotatorCLI.ANNOTATION, "HomopolymerRun")
                .addArgument(GATKVariantAnnotatorCLI.ANNOTATION, "SpanningDeletions")
                .addArgument(GATKVariantAnnotatorCLI.BAM, picardMarkDuplicatesOutput.getName())
                .addArgument(GATKVariantAnnotatorCLI.REFERENCESEQUENCE, "<referenceSequence>")
                .addArgument(GATKVariantAnnotatorCLI.OUT, gatkVariantAnnotatorOutput.getName()).build();
        graph.addVertex(gatkVCFJob);
        graph.addEdge(picardSortVCFJob, gatkVCFJob);

        // new job
        File filterVariantOutput = new File(gatkVariantAnnotatorOutput.getName().replace(".vcf", ".ic.vcf"));
        CondorJob filterVariantJob = new CondorJobBuilder().name(String.format("%s_%d", FilterVariantCLI.class.getSimpleName(), ++count))
                .addArgument(FilterVariantCLI.INTERVALLIST, "<icSNPIntervalList>")
                .addArgument(FilterVariantCLI.INPUT, gatkVariantAnnotatorOutput.getName())
                .addArgument(FilterVariantCLI.OUTPUT, filterVariantOutput.getName()).build();
        graph.addVertex(filterVariantJob);
        graph.addEdge(gatkVCFJob, filterVariantJob);

        // new job
        CondorJob removeJob = new CondorJobBuilder().name(String.format("%s_%d", RemoveCLI.class.getSimpleName(), ++count))
                .addArgument(RemoveCLI.FILE, mergeBAMFilesOut.getName()).addArgument(RemoveCLI.FILE, vcfFilterOutput.getName())
                .addArgument(RemoveCLI.FILE, picardAddOrReplaceReadGroupsOut.getName())
                .addArgument(RemoveCLI.FILE, mergeVCFOutput.getName()).addArgument(RemoveCLI.FILE, sortAndRemoveDuplicatesOutput.getName())
                .addArgument(RemoveCLI.FILE, sortAndRemoveDuplicatesOutput.getName().replace(".vcf", ".vcf.idx"))
                .addArgument(RemoveCLI.FILE, ploidyFile.getName()).build();
        graph.addVertex(removeJob);
        graph.addEdge(filterVariantJob, removeJob);

        CondorDOTExporter<CondorJob, CondorJobEdge> dotExporter = new CondorDOTExporter<CondorJob, CondorJobEdge>(a -> a.getName(), b -> {

            StringBuilder sb = new StringBuilder();
            sb.append(b.getName());
            if (StringUtils.isNotEmpty(b.getArgumentsClassAd().getValue())) {
                sb.append("\n");
                Pattern p = Pattern.compile("--\\w+(\\s[a-zA-Z_0-9<>\\.]+)?");
                Matcher m = p.matcher(b.getArgumentsClassAd().getValue());
                while (m.find()) {
                    sb.append(String.format("%s\n", m.group()));
                }
            }

            return sb.toString();

        }, null, null, null, null);
        File srcSiteResourcesImagesDir = new File("../src/site/resources/images");
        if (!srcSiteResourcesImagesDir.exists()) {
            srcSiteResourcesImagesDir.mkdirs();
        }
        File dotFile = new File(srcSiteResourcesImagesDir, "workflow.dag.dot");
        try {
            FileWriter fw = new FileWriter(dotFile);
            dotExporter.export(fw, graph);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
