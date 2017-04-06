package edu.unc.mapseq.workflow;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.junit.Test;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobBuilder;
import org.renci.jlrm.condor.CondorJobEdge;
import org.renci.jlrm.condor.ext.CondorDOTExporter;
import org.renci.jlrm.condor.ext.CondorJobVertexNameProvider;

import edu.unc.mapseq.module.core.RemoveCLI;
import edu.unc.mapseq.module.core.ZipCLI;
import edu.unc.mapseq.module.sequencing.SureSelectTriggerSplitterCLI;
import edu.unc.mapseq.module.sequencing.filter.FilterVariantCLI;
import edu.unc.mapseq.module.sequencing.freebayes.FreeBayesCLI;
import edu.unc.mapseq.module.sequencing.gatk3.GATKVariantAnnotatorCLI;
import edu.unc.mapseq.module.sequencing.picard.PicardAddOrReplaceReadGroupsCLI;
import edu.unc.mapseq.module.sequencing.picard.PicardMarkDuplicatesCLI;
import edu.unc.mapseq.module.sequencing.picard.PicardMergeSAMCLI;
import edu.unc.mapseq.module.sequencing.picard2.PicardSortVCFCLI;
import edu.unc.mapseq.module.sequencing.samtools.SAMToolsDepthCLI;
import edu.unc.mapseq.module.sequencing.samtools.SAMToolsFlagstatCLI;
import edu.unc.mapseq.module.sequencing.samtools.SAMToolsIndexCLI;
import edu.unc.mapseq.module.sequencing.vcflib.MergeVCFCLI;
import edu.unc.mapseq.module.sequencing.vcflib.SortAndRemoveDuplicatesCLI;
import edu.unc.mapseq.module.sequencing.vcflib.VCFFilterCLI;

public class NCNEXUSMergeBAMWorkflowTest {

    public NCNEXUSMergeBAMWorkflowTest() throws WorkflowException {
        super();
    }

    @Test
    public void createGraph() throws WorkflowException {

        int count = 0;

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(CondorJobEdge.class);

        // // new job
        CondorJob mergeBAMFilesJob = new CondorJobBuilder().name(String.format("%s_%d", PicardMergeSAMCLI.class.getSimpleName(), ++count))
                .build();
        graph.addVertex(mergeBAMFilesJob);

        // new job
        CondorJob picardAddOrReplaceReadGroupsJob = new CondorJobBuilder()
                .name(String.format("%s_%d", PicardAddOrReplaceReadGroupsCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(picardAddOrReplaceReadGroupsJob);
        graph.addEdge(mergeBAMFilesJob, picardAddOrReplaceReadGroupsJob);

        // new job
        CondorJob picardMarkDuplicatesJob = new CondorJobBuilder()
                .name(String.format("%s_%d", PicardMarkDuplicatesCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(picardMarkDuplicatesJob);
        graph.addEdge(picardAddOrReplaceReadGroupsJob, picardMarkDuplicatesJob);

        // new job
        CondorJob samtoolsIndexJob = new CondorJobBuilder().name(String.format("%s_%d", SAMToolsIndexCLI.class.getSimpleName(), ++count))
                .build();
        graph.addVertex(samtoolsIndexJob);
        graph.addEdge(picardMarkDuplicatesJob, samtoolsIndexJob);

        // new job
        CondorJob samtoolsFlagstatJob = new CondorJobBuilder()
                .name(String.format("%s_%d", SAMToolsFlagstatCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(samtoolsFlagstatJob);
        graph.addEdge(samtoolsIndexJob, samtoolsFlagstatJob);

        // new job
        CondorJob zipJob = new CondorJobBuilder().name(String.format("%s_%d", ZipCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(zipJob);
        graph.addEdge(samtoolsIndexJob, zipJob);

        // new job
        CondorJob samtoolsDepthJob = new CondorJobBuilder().name(String.format("%s_%d", SAMToolsDepthCLI.class.getSimpleName(), ++count))
                .build();
        graph.addVertex(samtoolsDepthJob);
        graph.addEdge(samtoolsIndexJob, samtoolsDepthJob);

        // new job
        CondorJob picardCollectHsMetricsJob = new CondorJobBuilder().name(String.format("%s_%d", ZipCLI.class.getSimpleName(), ++count))
                .build();
        graph.addVertex(picardCollectHsMetricsJob);
        graph.addEdge(samtoolsIndexJob, picardCollectHsMetricsJob);

        // new job
        CondorJob sureSelectTriggerSplitterJob = new CondorJobBuilder()
                .name(String.format("%s_%d", SureSelectTriggerSplitterCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(sureSelectTriggerSplitterJob);
        graph.addEdge(samtoolsIndexJob, sureSelectTriggerSplitterJob);

        List<CondorJob> mergeVCFParentJobs = new ArrayList<CondorJob>();

        for (int i = 0; i < 4; i++) {

            // new job
            CondorJob freeBayesJob = new CondorJobBuilder().name(String.format("%s_%d", FreeBayesCLI.class.getSimpleName(), ++count))
                    .build();
            graph.addVertex(freeBayesJob);
            graph.addEdge(sureSelectTriggerSplitterJob, freeBayesJob);
            mergeVCFParentJobs.add(freeBayesJob);

        }

        // new job
        CondorJob mergeVCFJob = new CondorJobBuilder().name(String.format("%s_%d", MergeVCFCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(mergeVCFJob);
        for (CondorJob job : mergeVCFParentJobs) {
            graph.addEdge(job, mergeVCFJob);
        }

        // new job
        CondorJob vcfFilterJob = new CondorJobBuilder().name(String.format("%s_%d", VCFFilterCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(vcfFilterJob);
        graph.addEdge(mergeVCFJob, vcfFilterJob);

        // new job
        CondorJob sortAndRemoveDuplicatesJob = new CondorJobBuilder()
                .name(String.format("%s_%d", SortAndRemoveDuplicatesCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(sortAndRemoveDuplicatesJob);
        graph.addEdge(vcfFilterJob, sortAndRemoveDuplicatesJob);

        // new job
        CondorJob picardSortVCFJob = new CondorJobBuilder().name(String.format("%s_%d", PicardSortVCFCLI.class.getSimpleName(), ++count))
                .build();
        graph.addVertex(picardSortVCFJob);
        graph.addEdge(sortAndRemoveDuplicatesJob, picardSortVCFJob);

        // new job
        CondorJob gatkVCFJob = new CondorJobBuilder().name(String.format("%s_%d", GATKVariantAnnotatorCLI.class.getSimpleName(), ++count))
                .build();
        graph.addVertex(gatkVCFJob);
        graph.addEdge(picardSortVCFJob, gatkVCFJob);

        // new job
        CondorJob filterVariantJob = new CondorJobBuilder().name(String.format("%s_%d", FilterVariantCLI.class.getSimpleName(), ++count))
                .build();
        graph.addVertex(filterVariantJob);
        graph.addEdge(gatkVCFJob, filterVariantJob);

        // new job
        CondorJob removeJob = new CondorJobBuilder().name(String.format("%s_%d", RemoveCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(removeJob);
        graph.addEdge(filterVariantJob, removeJob);

        CondorJobVertexNameProvider vnp = new CondorJobVertexNameProvider();
        CondorDOTExporter<CondorJob, CondorJobEdge> dotExporter = new CondorDOTExporter<CondorJob, CondorJobEdge>(vnp, vnp, null, null,
                null, null);
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
