package eu.cloudbutton.utslambda.forkjoin;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

import eu.cloudbutton.utslambda.Bag;
import eu.cloudbutton.utslambda.Utils;

public class BagRecursiveTask extends RecursiveTask<Long> {
    private static final long serialVersionUID = 1L;

    private List<Bag> bags;
 
    public BagRecursiveTask(List<Bag> bags) {
        this.bags = bags;
    }
    
    
    @Override
    protected Long compute() {
        Utils.resizeBags(bags, UTSForkJoin.PARALLELLISM);
        
        if (bags.size() > 1) {
            return ForkJoinTask.invokeAll(createSubtasks())
              .stream()
              .mapToLong(ForkJoinTask::join)
              .sum();
        } else {
            //System.out.println(Thread.currentThread().getId());
            Bag bag = processBag(bags.get(0));
            if (bag.size==0) {
                return bag.count;
            } else {
                //bags = new ArrayList<Bag>();
                bags.clear();
                bags.add(bag);
                return this.compute();
            }
        }
    }
 
    private Collection<BagRecursiveTask> createSubtasks() {
        List<BagRecursiveTask> dividedTasks = new ArrayList<>();
        for (Bag bag : bags) {
            List<Bag> newBagList = new ArrayList<Bag>();
            newBagList.add(bag);
            dividedTasks.add(new BagRecursiveTask(newBagList));
        }
        return dividedTasks;
    }
 
    private Bag processBag(Bag bag) {
        //long init = System.nanoTime();
        MessageDigest md = Utils.encoder();

        for (int n = UTSForkJoin.NUMBER_OF_ITERATIONS; n > 0 && bag.size > 0; --n) {
            bag.expand(md);
        }
        //long end = System.nanoTime();
        //UTSForkJoin.actualComputeTimes.add(end-init);
        
        return bag;
    }
}