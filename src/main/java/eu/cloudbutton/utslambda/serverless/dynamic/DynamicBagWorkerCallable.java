package eu.cloudbutton.utslambda.serverless.dynamic;

import java.io.Serializable;
import java.security.MessageDigest;
import java.util.concurrent.Callable;

import eu.cloudbutton.utslambda.Bag;
import eu.cloudbutton.utslambda.Utils;

public class DynamicBagWorkerCallable implements Callable<Bag>, Serializable {

    private static final long serialVersionUID = 1L;
    
    private Bag bag;
    private int numberOfIterations;

    public DynamicBagWorkerCallable(Bag bag, int numberOfIterations) {
        this.bag = bag;
        this.numberOfIterations = numberOfIterations;
    }

    @Override
    public Bag call() throws Exception {
        MessageDigest md = Utils.encoder();

        for (int n = numberOfIterations; n > 0 && bag.size > 0; --n) {
            bag.expand(md);
        }        
        
        return bag;
    }

}
