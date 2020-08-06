package eu.cloudbutton.utslambda.serverless.taskmanager;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import com.amazonaws.regions.Regions;

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
public class TMServerlessPreSplitUTS {
    static {
        Config.functionName = "CloudTread-utslambda";
        Config.region = Regions.EU_WEST_1;
        //Config.region = Regions.US_EAST_1;
    }

    public static long run(final int parallelism, final int numberOfIterationsPerWave, final int depth) {
        refTs = System.currentTimeMillis();
        TMServerlessPreSplitUTS waves = new TMServerlessPreSplitUTS(parallelism, numberOfIterationsPerWave);
        waves.run(depth);
        
        return waves.counter.get();
    }

    private TMServerlessPreSplitUTS(final int parallelism, final int numberOfIterationsPerWave) {

        this.parallelism = parallelism;
        this.numberOfIterationsPerWave = numberOfIterationsPerWave;
        this.counter = new AtomicLong(0);
        this.activeThreads = new AtomicLong(0); 
    }

    private static ServerlessExecutorService executorService;
    private static ExecutorService localExecutorService;

    private /*final*/ int parallelism;
    private /*final*/ int numberOfIterationsPerWave;
    // private long count;
    private int step = 0;
    
    private Random random = new Random();

    private void run(List<Bag> bags) {

        presplit(bags);
        
        //Utils.resizeBags(bags, 2000);
        parallelize(bags, bags.size());
        
        
        while (activeThreads.get() > 0 || !queue.isEmpty()) {
            //System.out.println(activeThreads.get());
            Bag bag = queue.poll();
            if (bag != null) {
                //System.out.println("TM: I found a bag in the queue!");
                long currentActiveThreads = activeThreads.addAndGet(-1);
                if (random.nextDouble() < 0.1) {
                    System.out.println(currentActiveThreads + " pending threads // actually " +
                        queue.size() + " bags in the queue");
                }
                
                // Trick
                if (step==0 && currentActiveThreads > 800) {
                    step++;
                    parallelism = 5;
                    numberOfIterationsPerWave = 2_500_000;
                    System.out.println("******* APPLIED STEP 0");
                }
                if (step==1 && currentActiveThreads > 1300) {
                    step++;
                    parallelism = 5;
                    numberOfIterationsPerWave = 5_000_000;
                    System.out.println("******* APPLIED STEP 1");
                }
                if (step==2 && currentActiveThreads < 1100) {
                    step++;
                    parallelism = 5;
                    numberOfIterationsPerWave = 2_500_000;
                    System.out.println("******* APPLIED STEP 2");
                }
                if (step==3 && currentActiveThreads < 100) {
                    step++;
                    parallelism = 5;
                    numberOfIterationsPerWave = 1_000_000;
                    System.out.println("******* APPLIED STEP 3");
                }
                
                
                /*if (currentActiveThreads > 1000-parallelism) {
                    System.out.println("Requeueing bag. Current active threads = " + currentActiveThreads);
                    queue.offer(bag);
                } else {*/
                
                Bag resultBag = coalesceAndCount(bag);
                if (resultBag != null) {
                    int parallelismParam = parallelism;
                    /*if (currentActiveThreads > 1000) {
                        parallelismParam = 2;
                    }*/
                    List<Bag> bags2 = new ArrayList<>();
                    bags2.add(resultBag);
                    Utils.resizeBags(bags2, parallelismParam);
                    parallelize(bags2, bags2.size());
                }
                //}
            }
        }
        
    }

    private void presplit(List<Bag> bags) {
        int STEPS = 6;
        int targetBags = 2000;
        int splitFactor = 5;
        
        MessageDigest md = Utils.encoder();
        
        for (int step = 0; step < STEPS; step++) {
            System.out.println("STEP " + step);
            for (int i = 0; i < bags.size(); i++) {
                Bag b = bags.get(i);
                for (int j=0;j<100 && b.size > 0;j++) {
                    b.expand(md);
                }
            }

            List<Bag> coalescedBags = new ArrayList<>();
            long tempCount = Utils.coalesceAndCount(bags, coalescedBags);
            counter.addAndGet(tempCount);
            System.out.println("End of round " + step+ "  count = "+ tempCount + "( "+ bags.size()+" --> "+coalescedBags.size()+")");
            
            bags.clear();
            
            for (Bag b : coalescedBags) {
                List<Bag> rBags = new ArrayList<>();
                rBags.add(b);
                Utils.resizeBags(rBags, splitFactor);
                bags.addAll(rBags);
            }
            
            System.out.println("Split to " + bags.size() + " bags");
            if (bags.size() >= targetBags) 
                break;
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
    //static List<Long> taskDurations = Collections.synchronizedList(new ArrayList<>());
    //static List<Map.Entry<Long, Long>> finishTimes = Collections.synchronizedList(new ArrayList<>());
    static List<TaskStats> taskStatsList = Collections.synchronizedList(new ArrayList<>());
    static Long refTs;
    
    
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
            TMResult tmResult = future.get();
            queue.offer(tmResult.getBag());
            //taskDurations.add(tmResult.getEndTs() - tmResult.getInitTs());
            //finishTimes.add(Map.entry(tmResult.getEndTs() - refTs, tmResult.getEndTs() - tmResult.getInitTs()));
            taskStatsList.add(new TaskStats(tmResult.getBag().bagId,
                    tmResult.getBag().parentBagId,
                    tmResult.getInitTs() - refTs,
                    tmResult.getEndTs() - tmResult.getInitTs()));
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
        
        //List<Future<Object>> futures = new ArrayList<>();
        for (int w = 0; w < size; w++) {
            Future<Object> future = localExecutorService
                    .submit(new LocalCallable(bags.get(w), numberOfIterationsPerWave));
            //futures.add(future);
            
        }

        System.out.println("Launched " + size + " remote functions...");

    }

    public static void main(String[] args) {

        final CmdLineOptions opts = CmdLineOptions.makeOrExit(args);

        try {
            executorService = new AWSLambdaExecutorService();
            executorService.setLogs(false);

            localExecutorService = Executors.newFixedThreadPool(2000);

            final int parallelism = 30;
            final int numberOfIterationsPerWave = 5_000_000;

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
            
            //PlotData.plotHistogram(taskDurations);
            //PlotData.plotScatterDuration(finishTimes);
            PlotData.plotConcurrency(taskStatsList);

        } finally {
            executorService.shutdown();
            localExecutorService.shutdown();
            System.out.println("finish");
        }
    }
}
