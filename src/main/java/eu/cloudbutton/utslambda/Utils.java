package eu.cloudbutton.utslambda;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class Utils {

    /**
     * coalesce empty bags. remove the count from each bag, and return the total
     *
     * @return the current count
     */
    public static long coalesceAndCount(List<Bag> src, List<Bag> target) {
        long count = 0;

        for (Bag b : src) {
            count += b.count;
            b.count = 0;
            if (b.size != 0) {
                target.add(b);
            }
        }
        return count;
    }

    public static MessageDigest encoder() {
        try {
            return MessageDigest.getInstance("SHA-1");
        } catch (final NoSuchAlgorithmException e) {
            System.err.println("Could not initialize a MessageDigest for the \"SHA-1\" algorithm");
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 
     * @param bags
     * @param targetSize
     */
    public static void resizeBags(List<Bag> bags, final int targetSize) {
        if (bags.isEmpty()) {
            return;
        }
        int currentSize = bags.size();
        if (currentSize >= targetSize) {
            return;
        }

        List<Bag> newBags = new ArrayList<Bag>();
        Iterator<Bag> bagIter = bags.iterator();
        while (currentSize < targetSize && bagIter.hasNext()) {
            Bag b = bagIter.next();
            while (currentSize < targetSize) {
                Bag sp = b.split();
                if (sp == null) {
                    // b can't be split anymore
                    break;
                } else {
                    newBags.add(sp);
                    currentSize++;
                }
            }
        }
        if (currentSize < targetSize) {
            // we need more bags
            // the original bags can't be split anymore
            // but we can try to resplit the newly split off bags
            resizeBags(newBags, targetSize - bags.size());
        }
        // add the new bags in
        // note that split destructively updated the old bags already
        bags.addAll(newBags);
    }

    public static String sub(final String str, final int start, final int end) {
        return str.substring(start, Math.min(end, str.length()));
    }

    public static List<List<Bag>> groupBags(List<Bag> bags, int groupSize) {
        List<List<Bag>> groupedBags = new ArrayList<>();
        for (int i=0; i<(bags.size()-1)/groupSize+1; i++ ) {
            groupedBags.add(new ArrayList<>(bags.subList(i*groupSize, Math.min((i+1)*groupSize,bags.size()))));
        }
        
        return groupedBags;
    }
}
