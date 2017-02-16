package edu.unc.mapseq.ws.ncnexus.mergebam;

import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.ws.Service;
import javax.xml.ws.soap.SOAPBinding;

import org.junit.Test;

import edu.unc.mapseq.ws.ncnexus38.mergevc.MetricsResult;
import edu.unc.mapseq.ws.ncnexus38.mergevc.NCNEXUS38MergeVCService;

public class NCNEXUSMergeBAMServiceTest {

    @Test
    public void testService() {
        QName serviceQName = new QName("http://mergebam.ncnexus.ws.mapseq.unc.edu", "NCNEXUSMergeBAMService");
        QName portQName = new QName("http://mergebam.ncnexus.ws.mapseq.unc.edu", "NCNEXUSMergeBAMPort");
        Service service = Service.create(serviceQName);
        String host = "152.19.198.146";
        service.addPort(portQName, SOAPBinding.SOAP11HTTP_MTOM_BINDING, String.format("http://%s:%d/cxf/NCNEXUSMergeBAMService", host, 8181));
        NCNEXUS38MergeVCService mergeBAMService = service.getPort(NCNEXUS38MergeVCService.class);
        List<MetricsResult> results = mergeBAMService.getMetrics("012259Sb");
        results.forEach(a -> System.out.println(a.toString()));
    }
}
