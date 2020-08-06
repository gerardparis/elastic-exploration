package eu.cloudbutton.utslambda.multithread;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import eu.cloudbutton.utslambda.Bag;
import eu.cloudbutton.utslambda.CmdLineOptions;
import eu.cloudbutton.utslambda.Utils;


/**
 * 
 * @author Gerard
 *
 */
public class MultithreadUTS {
    

    public static long run(final int parallelism, final int numberOfIterationsPerWave, final int depth) {
        
        MultithreadUTS waves = new MultithreadUTS(parallelism, numberOfIterationsPerWave);
        waves.run(depth);
        
        return waves.counter.get();
    }

    private MultithreadUTS(final int parallelism, final int numberOfIterationsPerWave) {

        this.parallelism = parallelism;
        this.numberOfIterationsPerWave = numberOfIterationsPerWave;
        this.counter = new AtomicLong(0);
        this.activeThreads = new AtomicLong(0); 
    }

    //private static ServerlessExecutorService executorService;
    private static ExecutorService localExecutorService;

    private final int parallelism;
    private final int numberOfIterationsPerWave;

    
    private Random random = new Random();

    private void run(List<Bag> bags) {

        Utils.resizeBags(bags, parallelism);
        parallelize(bags, bags.size());
        
        
        while (activeThreads.get() > 0 || !queue.isEmpty()) {
            
            Bag bag = queue.poll();
            if (bag != null) {
                
                long currentActiveThreads = activeThreads.addAndGet(-1);
                if (random.nextDouble() < 0.1) {
                    System.out.println(currentActiveThreads + " pending threads - " +
                        queue.size() + " bags in the queue");
                }
                
                Bag resultBag = coalesceAndCount(bag);
                if (resultBag != null) {
                    int parallelismParam = parallelism;
                    
                    List<Bag> bags2 = new ArrayList<>();
                    bags2.add(resultBag);
                    Utils.resizeBags(bags2, parallelismParam);
                    parallelize(bags2, bags2.size());
                }

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
    
    //private static ThreadLocal<MessageDigest> encoder = ThreadLocal.withInitial(() -> Utils.encoder());
    
    class LocalCallable implements Callable<Object> {

        private Bag bag;
        private int numberOfIterations;

        public LocalCallable(Bag bag, int numberOfIterations) {
            this.bag = bag;
            this.numberOfIterations = numberOfIterations;
        }

        @Override
        public Object call() throws Exception {
            MessageDigest md = Utils.encoder();
            //MessageDigest md = encoder.get();

            for (int n = numberOfIterations; n > 0 && bag.size > 0; --n) {
                bag.expand(md);
            }
            queue.offer(bag);
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

        activeThreads.addAndGet(size);
        
        for (int w = 0; w < size; w++) {
            localExecutorService.submit(new LocalCallable(bags.get(w), numberOfIterationsPerWave));
        }

    }

    public static void main(String[] args) {

        final CmdLineOptions opts = CmdLineOptions.makeOrExit(args);

        try {
            System.out.println("workers set to " + opts.parallelism);
            localExecutorService = Executors.newFixedThreadPool(opts.parallelism);

            final int parallelism = 5; //opts.parallelism;
            final int numberOfIterationsPerWave = 5_000_000;

            
            System.out.println("Starting...");
            long time = -System.nanoTime();
            final long count = run(parallelism, numberOfIterationsPerWave, opts.depth);
            time += System.nanoTime();
            System.out.println("Finished.");

            System.out
                    .println("Depth: " + opts.depth + ", Performance: " + count + "/" + Utils.sub("" + time / 1e9, 0, 6)
                            + " = " + Utils.sub("" + (count / (time / 1e3)), 0, 6) + "M nodes/s");
            

        } finally {
            localExecutorService.shutdown();
            System.out.println("finish");
        }
    }
}
