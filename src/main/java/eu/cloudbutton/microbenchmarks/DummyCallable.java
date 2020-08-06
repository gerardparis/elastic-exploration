package eu.cloudbutton.microbenchmarks;

import java.io.Serializable;
import java.util.concurrent.Callable;

public class DummyCallable implements Callable<Result>, Serializable {

    private String inputParam;

    public DummyCallable(String inputParam){
        this.inputParam = inputParam;
    }

    @Override
    public Result call() throws Exception {
        return new Result(inputParam + " world!" );
    }
}
