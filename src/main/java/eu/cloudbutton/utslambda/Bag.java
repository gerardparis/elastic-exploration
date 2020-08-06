package eu.cloudbutton.utslambda;

import java.io.Serializable;
import java.security.DigestException;
import java.security.MessageDigest;
import java.util.UUID;

public final class Bag implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private static final double BRANCHING_FACTOR = 4.0;

    static double den = Math.log(BRANCHING_FACTOR / (1.0 + BRANCHING_FACTOR));

    byte[] hash;
    int[] depth;
    int[] lower;
    int[] upper;
    public int size = 0;
    public long count = 0L;

    // Used for logging purposes
    public UUID bagId;
    public UUID parentBagId;

    public Bag() {
    }

    public Bag(final int n) {
        this.hash = new byte[n * 20 + 4]; // slack for in-place SHA1 computation
        this.depth = new int[n];
        this.lower = new int[n];
        this.upper = new int[n];
    }

    void digest(final MessageDigest md, final int d) {
        if (size >= depth.length) {
            grow();
        }
        ++count;
        final int offset = size * 20;

        try {
            md.digest(hash, offset, 20);
        } catch (DigestException ex) {
        }

        final int v = ((0x7f & hash[offset + 16]) << 24) | ((0xff & hash[offset + 17]) << 16)
                | ((0xff & hash[offset + 18]) << 8) | (0xff & hash[offset + 19]);
        final int n = (int) (Math.log(1.0 - v / 2147483648.0) / den);
        if (n > 0) {
            if (d > 1) {
                depth[size] = d - 1;
                lower[size] = 0;
                upper[size++] = n;
            } else {
                count += n;
            }
        }
    }

    public void seed(final MessageDigest md, final int s, final int d) {

        for (int i = 0; i < 16; ++i) {
            hash[i] = (byte) 0;
        }
        hash[16] = (byte) (s >> 24);
        hash[17] = (byte) (s >> 16);
        hash[18] = (byte) (s >> 8);
        hash[19] = (byte) s;
        md.update(hash, 0, 20);
        
        digest(md, d);

    }

    public void expand(final MessageDigest md) {
        final int top = size - 1;
        final int d = depth[top];
        final int l = lower[top];
        final int u = upper[top] - 1;
        if (u == l) {
            size = top;
        } else {
            upper[top] = u;
        }
        final int offset = top * 20;
        hash[offset + 20] = (byte) (u >> 24);
        hash[offset + 21] = (byte) (u >> 16);
        hash[offset + 22] = (byte) (u >> 8);
        hash[offset + 23] = (byte) u;
        md.update(hash, offset, 24);
        digest(md, d);
    }

    void run(final MessageDigest md) {

        while (size > 0) {
            expand(md);
        }

    }

    Bag trim() {
        final Bag b;
        if (size == 0) {
            b = new Bag();
        } else {
            b = new Bag(size);
            System.arraycopy(hash, 0, b.hash, 0, size * 20);
            System.arraycopy(depth, 0, b.depth, 0, size);
            System.arraycopy(lower, 0, b.lower, 0, size);
            System.arraycopy(upper, 0, b.upper, 0, size);
            b.size = size;
        }
        b.count = count;
        return b;
    }

    Bag split() {
        int s = 0;
        for (int i = 0; i < size; ++i) {
            if ((upper[i] - lower[i]) >= 2) {
                ++s;
            }
        }
        if (s == 0) {
            return null;
        }
        final Bag b = new Bag(s);
        for (int i = 0; i < size; ++i) {
            final int p = upper[i] - lower[i];
            if (p >= 2) {
                System.arraycopy(hash, i * 20, b.hash, b.size * 20, 20);
                b.depth[b.size] = depth[i];
                b.upper[b.size] = upper[i];
                upper[i] -= p / 2;
                b.lower[b.size++] = upper[i];
            }
        }
        b.parentBagId = bagId;
        b.bagId = UUID.randomUUID();
        return b;
    }

    void merge(final Bag b) {
        final int s = size + b.size;
        while (s > depth.length)
            grow();
        System.arraycopy(b.hash, 0, hash, size * 20, b.size * 20);
        System.arraycopy(b.depth, 0, depth, size, b.size);
        System.arraycopy(b.lower, 0, lower, size, b.size);
        System.arraycopy(b.upper, 0, upper, size, b.size);
        size = s;
    }

    void grow() {
        final int n = depth.length * 2;
        final byte[] h = new byte[n * 20 + 4];
        final int[] d = new int[n];
        final int[] l = new int[n];
        final int[] u = new int[n];
        System.arraycopy(hash, 0, h, 0, size * 20);
        System.arraycopy(depth, 0, d, 0, size);
        System.arraycopy(lower, 0, l, 0, size);
        System.arraycopy(upper, 0, u, 0, size);
        hash = h;
        depth = d;
        lower = l;
        upper = u;
    }

    /*
     * Simple sequential
     */
    public static void main(String[] args) {

        final CmdLineOptions opts = CmdLineOptions.makeOrExit(args);

        final MessageDigest md = Utils.encoder();

        Bag bag = new Bag(64);

        if (opts.warmupDepth > 0) {
            System.out.println("Warmup...");
            bag.seed(md, 19, opts.warmupDepth - 2);
            bag.run(md);

            bag = new Bag(64);
        }

        System.out.println("Starting...");
        long time = -System.nanoTime();

        bag.seed(md, 19, opts.depth);
        bag.run(md);

        time += System.nanoTime();
        System.out.println("Finished.");

        final long count = bag.count;
        System.out.println("Depth: " + opts.depth + ", Performance: " + count + "/" + Utils.sub("" + time / 1e9, 0, 6)
                + " = " + Utils.sub("" + (count / (time / 1e3)), 0, 6) + "M nodes/s");
    }
}
