package eu.cloudbutton.utslambda;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class DynamicUTS {

    public static AtomicLong run(final int parallelism, final int numberOfIterationsPerWave, final int depth) {
        DynamicUTS waves = new DynamicUTS(parallelism, numberOfIterationsPerWave);
        waves.run(depth);
        return waves.counter;
    }

    private DynamicUTS(final int parallelism, final int numberOfIterationsPerWave) {

        this.parallelism = parallelism;
        this.numberOfIterationsPerWave = numberOfIterationsPerWave;
        this.count = 0;
    }

    private final int parallelism;
    private final int numberOfIterationsPerWave;
    private long count;

    private AtomicLong counter = new AtomicLong(0);

    // private void run(List<Bag> bags) {
    // bags = doSimpleRound(bags);
//
    // }

    private void run(final int depth) {
        final Bag initBag = new Bag(64);
        final MessageDigest md = Utils.encoder();
        initBag.seed(md, 19, depth);

        final List<Bag> bags = new ArrayList<Bag>();
        bags.add(initBag);
        Utils.resizeBags(bags, parallelism);

        parallelize(bags, bags.size());

        System.out.println("End of round count: " + this.counter);
    }

    public void parallelize(List<Bag> bags, int size) {
        ArrayList<Thread> threads = new ArrayList<>();

        // long initTime = System.currentTimeMillis();
        for (int w = 0; w < size; w++) {
            threads.add(new Thread(new DynamicWorker(bags.get(w), counter, parallelism)));
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

    public static void main(String[] args) {

        final CmdLineOptions opts = CmdLineOptions.makeOrExit(args);

        try {
            // final int parallelism = opts.parallelism final int parallelism = 1 ;

            final int parallelism = 128;
            final int numberOfIterationsPerWave = 5000000;
            // final int numberOfIterationsPerWave = 1;
            if (opts.warmupDepth > 0) {
                System.out.println("Warmup...");
                run(parallelism, numberOfIterationsPerWave, opts.warmupDepth);
                // run(parallelism, numberOfIterationsPerWave, 4);
            }

            System.out.println("Starting...");
            long time = -System.nanoTime();

            // final long count = run(parallelism, numberOfIterationsPerWave, opts.depth);

            // final long count = run(parallelism, numberOfIterationsPerWave, 4).get();
            final long count = run(parallelism, numberOfIterationsPerWave, opts.depth).get();
            time += System.nanoTime();
            System.out.println("Finished.");

            System.out
                    .println("Depth: " + opts.depth + ", Performance: " + count + "/" + Utils.sub("" + time / 1e9, 0, 6)
                            + " = " + Utils.sub("" + (count / (time / 1e3)), 0, 6) + "M nodes/s");
        } finally {
            // jsc.stop();
            System.out.println("finish");
        }
    }
}
