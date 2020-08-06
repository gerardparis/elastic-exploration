package eu.cloudbutton.utslambda;

import java.security.MessageDigest;
import java.util.List;

public class BagWorker implements Runnable {

    private List<Bag> result;
    private Bag bag;
    private int numberOfIterations;

    public BagWorker(Bag task, List<Bag> result, int numberOfIterations) {
        this.result = result;
        this.bag = task;
        this.numberOfIterations = numberOfIterations;
    }

    public void run() {

        MessageDigest md = Utils.encoder();

        int n = numberOfIterations;
        for (; n > 0 && bag.size > 0; --n) {
            bag.expand(md);
        }

        result.add(bag);

    }

}
