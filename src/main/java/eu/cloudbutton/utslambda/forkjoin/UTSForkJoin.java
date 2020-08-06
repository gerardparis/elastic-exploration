package eu.cloudbutton.utslambda.forkjoin;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;

import eu.cloudbutton.utslambda.Bag;
import eu.cloudbutton.utslambda.Utils;

public class UTSForkJoin {
    
    public static int depth = 15;
    public static int PARALLELLISM = 5;
    public static int NUMBER_OF_ITERATIONS = 5_000_000; //5_000_000;
    
    //public static List<Long> actualComputeTimes = Collections.synchronizedList(new ArrayList<>()); 

    public UTSForkJoin() {
        ForkJoinPool workStealingPool = (ForkJoinPool) Executors.newWorkStealingPool();
        
        System.out.println("Starting...");
        
        final Bag initBag = new Bag(64);
        final MessageDigest md = Utils.encoder();
        initBag.seed(md, 19, depth);

        final List<Bag> bags = new ArrayList<Bag>();
        bags.add(initBag);
        BagRecursiveTask customRecursiveTask = new BagRecursiveTask(bags);
        long time = -System.nanoTime();
        Long count = workStealingPool.invoke(customRecursiveTask);
        time += System.nanoTime();
        
        System.out.println("Finished.");
        
        System.out.println("StealCount: " + workStealingPool.getStealCount());

        System.out
                .println("Depth: " + depth + ", Performance: " + count + "/" + Utils.sub("" + time / 1e9, 0, 6)
                        + " = " + Utils.sub("" + (count / (time / 1e3)), 0, 6) + "M nodes/s");
        
        //printActualComputeTime();

    }
    
    /*private static void printActualComputeTime() {
        long actualTime = 0L;
        for (Long e : actualComputeTimes) {
            actualTime += e;
        }
        System.out.println("Actual compute time (s): " + Utils.sub("" + actualTime / 1e9, 0, 6));
        
    }*/

    public static void main(String[] args) {
        new UTSForkJoin();
    }
}
