package eu.cloudbutton.utslambda;

import java.io.Serializable;
import java.security.MessageDigest;
import java.util.List;
import java.util.concurrent.Callable;

public class BagWorkerCallable implements Callable<List<Bag>>, Serializable {

    private static final long serialVersionUID = 1L;
    
    private List<Bag> bags;
    private int numberOfIterations;

    public BagWorkerCallable(List<Bag> bags, int numberOfIterations) {
        this.bags = bags;
        this.numberOfIterations = numberOfIterations;
    }

    @Override
    public List<Bag> call() throws Exception {
        MessageDigest md = Utils.encoder();

        for (Bag bag : bags) {
            for (int n = numberOfIterations; n > 0 && bag.size > 0; --n) {
                bag.expand(md);
            }
        }
        
        return bags;
    }

}
