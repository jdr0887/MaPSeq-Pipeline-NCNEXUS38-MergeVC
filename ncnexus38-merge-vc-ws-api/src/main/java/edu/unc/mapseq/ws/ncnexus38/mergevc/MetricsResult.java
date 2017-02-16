package edu.unc.mapseq.ws.ncnexus38.mergevc;

import java.io.Serializable;

public class MetricsResult implements Serializable {

    private static final long serialVersionUID = 3860068031497779345L;

    private String key;

    private String value;

    public MetricsResult() {
        super();
    }

    public MetricsResult(String key, String value) {
        super();
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return String.format("MetricsResult [key=%s, value=%s]", key, value);
    }

}
