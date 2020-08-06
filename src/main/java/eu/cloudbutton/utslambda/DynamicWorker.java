package eu.cloudbutton.utslambda;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class DynamicWorker implements Runnable {

    private Bag bag;
    private int numberOfIterations;
    private AtomicLong counter;
    private int parallelism;

    public DynamicWorker(Bag task, AtomicLong counter, int parallelism) {
        this.bag = task;
        this.numberOfIterations = 5000000;
        this.counter = counter;
        this.parallelism = parallelism;
    }

    public void run() {

        // System.out.println("WORKER");

        MessageDigest md = Utils.encoder();

        int n = numberOfIterations;
        for (; n > 0 && bag.size > 0; --n) {
            bag.expand(md);
        }

        // this.count += Utils.coalesceAndCount(results, compactedBags);

        Bag result = coalesceAndCount(bag);

        if (result != null) {
            List<Bag> bags = new ArrayList<>();
            bags.add(bag);
            Utils.resizeBags(bags, parallelism);
            parallelize(bags, bags.size());
        }

        // result.add(bag);

    }

    public Bag coalesceAndCount(Bag b) {

        counter.addAndGet(b.count);
        b.count = 0;
        if (b.size != 0)
            return b;
        else
            return null;

    }

    public void parallelize(List<Bag> bags, int size) {
        ArrayList<Thread> threads = new ArrayList<>();

        // long initTime = System.currentTimeMillis();
        for (int w = 0; w < size; w++) {
            threads.add(new Thread(new DynamicWorker(bags.get(w), counter, size)));
        }

        System.out.println("Launching " + threads.size() + " threads...");
        for (Thread t : threads)
            t.start();
        
        try {
            for (Thread t : threads)
                t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        // long endTime = System.currentTimeMillis();

    }

}
