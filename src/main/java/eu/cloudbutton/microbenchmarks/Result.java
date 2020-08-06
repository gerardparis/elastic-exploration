package eu.cloudbutton.microbenchmarks;

import java.io.Serializable;

public class Result implements Serializable {

    private static final long serialVersionUID = 1L;

    private String outputParam;

    public Result(String outputParam) {
        this.outputParam = outputParam;
    }

    public String getOutputParam() {
        return outputParam;
    }

    public void setOutputParam(String outputParam) {
        this.outputParam = outputParam;
    }
}
