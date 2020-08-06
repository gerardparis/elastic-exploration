package eu.cloudbutton.utslambda;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

public class ParallelUTS {
    public static long run(final int parallelism, final int numberOfIterationsPerWave, final int depth) {
        ParallelUTS waves = new ParallelUTS(parallelism, numberOfIterationsPerWave);
        waves.run(depth);
        return waves.count;
    }

    private ParallelUTS(final int parallelism, final int numberOfIterationsPerWave) {

        this.parallelism = parallelism;
        this.numberOfIterationsPerWave = numberOfIterationsPerWave;
        this.count = 0;
    }

    private final int parallelism;
    private final int numberOfIterationsPerWave;
    private long count;

    private void run(List<Bag> bags) {
        // Utils.resizeBags(bags, parallelism);
        while (!bags.isEmpty()) {
            bags = doSimpleRound(bags);

        }
    }

    private void run(final int depth) {
        final Bag initBag = new Bag(64);
        final MessageDigest md = Utils.encoder();
        initBag.seed(md, 19, depth);

        final List<Bag> bags = new ArrayList<Bag>();
        bags.add(initBag);
        run(bags);
    }

    public List<Bag> doSimpleRound(List<Bag> bags) {

        Utils.resizeBags(bags, parallelism);

        //final int numberOfIterationsPerWave = this.numberOfIterationsPerWave;
        ArrayList<Bag> results = new ArrayList<>();
        parallelize(bags, results, bags.size());

        List<Bag> compactedBags = new ArrayList<Bag>(results.size());

        this.count += Utils.coalesceAndCount(results, compactedBags);
        System.out.println("End of round count: " + this.count);
        // System.out.println("Ecompacted bags: " + compactedBags.size());
        return compactedBags;

    }

    public void parallelize(List<Bag> bags, List<Bag> results, int size) {
        ArrayList<Thread> threads = new ArrayList<>();

        // long initTime = System.currentTimeMillis();
        for (int w = 0; w < size; w++) {
            threads.add(new Thread(new BagWorker(bags.get(w), results, numberOfIterationsPerWave)));
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

        System.out.println("Sizes of bags:");
        for(Bag b : results) {
            System.out.print(b.size + " ");
        }
        System.out.println("");
        
    }

    public static void main(String[] args) {

        final CmdLineOptions opts = CmdLineOptions.makeOrExit(args);

        try {
            // final int parallelism = opts.parallelism final int parallelism = 1 ;
            
            final int parallelism = opts.parallelism;
            final int numberOfIterationsPerWave = 5_000_000;
            // final int numberOfIterationsPerWave = 1;
            if (opts.warmupDepth > 0) {
                System.out.println("Warmup...");
                run(parallelism, numberOfIterationsPerWave, opts.warmupDepth);
            }

            System.out.println("Starting...");
            long time = -System.nanoTime();
            final long count = run(parallelism, numberOfIterationsPerWave, opts.depth);
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
