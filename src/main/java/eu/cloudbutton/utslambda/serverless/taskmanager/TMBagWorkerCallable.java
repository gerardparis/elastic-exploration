package eu.cloudbutton.utslambda.serverless.taskmanager;

import java.io.Serializable;
import java.security.MessageDigest;
import java.util.concurrent.Callable;

import eu.cloudbutton.utslambda.Bag;
import eu.cloudbutton.utslambda.Utils;

public class TMBagWorkerCallable implements Callable<TMResult>, Serializable {

    private static final long serialVersionUID = 1L;
    
    private Bag bag;
    private int numberOfIterations;

    public TMBagWorkerCallable(Bag bag, int numberOfIterations) {
        this.bag = bag;
        this.numberOfIterations = numberOfIterations;
    }

    @Override
    public TMResult call() throws Exception {
        long init = System.currentTimeMillis();
        MessageDigest md = Utils.encoder();

        for (int n = numberOfIterations; n > 0 && bag.size > 0; --n) {
            bag.expand(md);
        }
        long end = System.currentTimeMillis();
        
        
        return new TMResult(bag, init, end);
    }

}
