package eu.cloudbutton.utslambda.serverless.taskmanager;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import crucial.execution.ServerlessExecutorService;
import crucial.execution.aws.AWSLambdaExecutorService;
import crucial.execution.aws.Config;
import eu.cloudbutton.utslambda.Bag;
import eu.cloudbutton.utslambda.CmdLineOptions;
import eu.cloudbutton.utslambda.Utils;


/**
 * 
 * @author Gerard
 *
 */
public class TMPollNServerlessUTS {
    static {
        Config.functionName = "CloudTread-utslambda";
    }

    public static long run(final int parallelism, final int numberOfIterationsPerWave, final int depth) {
        TMPollNServerlessUTS waves = new TMPollNServerlessUTS(parallelism, numberOfIterationsPerWave);
        waves.run(depth);
        return waves.counter.get();
    }

    private TMPollNServerlessUTS(final int parallelism, final int numberOfIterationsPerWave) {

        this.parallelism = parallelism;
        this.numberOfIterationsPerWave = numberOfIterationsPerWave;
        this.counter = new AtomicLong(0);
        this.activeThreads = new AtomicLong(0); 
    }

    private static ServerlessExecutorService executorService;
    private static ExecutorService localExecutorService;

    private final int parallelism;
    private final int numberOfIterationsPerWave;
    // private long count;
    
    private Random random = new Random();
    private int pollingUpperBound = 5;

    private void run(List<Bag> bags) {

        Utils.resizeBags(bags, parallelism);
        parallelize(bags, bags.size());
        
        
        while (activeThreads.get() > 0 || !queue.isEmpty()) {
            //System.out.println(activeThreads.get());
            List<Bag> polledBags = new ArrayList<>(pollingUpperBound);
            for (int i=0;i<pollingUpperBound && !queue.isEmpty(); i++) {
                Bag bag = queue.poll();
                polledBags.add(bag);
                activeThreads.addAndGet(-1);
            }
            if (!polledBags.isEmpty()) {
                //System.out.println("TM: I found a bag in the queue!");
                if (random.nextDouble() < 0.1) {
                    System.out.println(activeThreads.get() + " bags still in the queue");
                }
                
                /*if (currentActiveThreads > 1000-parallelism) {
                    System.out.println("Requeueing bag. Current active threads = " + currentActiveThreads);
                    queue.offer(bag);
                } else {*/
                List<Bag> compactedBags = new ArrayList<>(polledBags.size());
                long c = Utils.coalesceAndCount(polledBags, compactedBags);
                counter.addAndGet(c);
                if (compactedBags.size() > 0) {
                    Utils.resizeBags(compactedBags, parallelism);
                    parallelize(compactedBags, compactedBags.size());
                }
                
                //}
            }
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

    AtomicLong counter;
    AtomicLong activeThreads;
    Deque<Bag> queue = new ConcurrentLinkedDeque<>();
    
    class LocalCallable implements Callable<Object> {

        private Bag bag;
        private int numberOfIterations;

        public LocalCallable(Bag bag, int numberOfIterations) {
            this.bag = bag;
            this.numberOfIterations = numberOfIterations;
        }

        @Override
        public Object call() throws Exception {
            Future<TMResult> future = executorService.submit(new TMBagWorkerCallable(bag, numberOfIterations));
            Bag resultBag = future.get().getBag();
            queue.offer(resultBag);
            return null;
        }
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

        // List<Callable<Bag>> myTasks = Collections.synchronizedList(new
        // ArrayList<>());

        activeThreads.addAndGet(size);
        
        List<Future<Object>> futures = new ArrayList<>();
        for (int w = 0; w < size; w++) {
            Future<Object> future = localExecutorService
                    .submit(new LocalCallable(bags.get(w), numberOfIterationsPerWave));
            futures.add(future);
            
        }

        System.out.println("Launched " + size + " remote functions...");

    }

    public static void main(String[] args) {

        final CmdLineOptions opts = CmdLineOptions.makeOrExit(args);

        try {
            executorService = new AWSLambdaExecutorService();
            executorService.setLogs(false);

            localExecutorService = Executors.newFixedThreadPool(1000);

            final int parallelism = 10;
            final int numberOfIterationsPerWave = 5000000;

            /*
             * // Warmup phase if (opts.warmupDepth > 0) { System.out.println("Warmup...");
             * run(parallelism, numberOfIterationsPerWave, opts.warmupDepth);
             * executorService.resetCostReport(); }
             */

            System.out.println("Starting...");
            long time = -System.nanoTime();
            final long count = run(parallelism, numberOfIterationsPerWave, opts.depth);
            time += System.nanoTime();
            System.out.println("Finished.");

            System.out
                    .println("Depth: " + opts.depth + ", Performance: " + count + "/" + Utils.sub("" + time / 1e9, 0, 6)
                            + " = " + Utils.sub("" + (count / (time / 1e3)), 0, 6) + "M nodes/s");
            System.out.println(executorService.printCostReport());

        } finally {
            executorService.shutdown();
            localExecutorService.shutdown();
            System.out.println("finish");
        }
    }
}
