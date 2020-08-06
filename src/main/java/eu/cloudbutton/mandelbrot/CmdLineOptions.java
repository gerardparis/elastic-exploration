package eu.cloudbutton.mandelbrot;

public class CmdLineOptions {
    public final int width;
    public final int height;
    public final int workers;

    /**
     * Warning: prints error / help message and calls {@link System#exit(int)} if
     * the arguments are not well formed
     * 
     * @param args The command line arguments
     * @return The parsed options
     */
    public static CmdLineOptions makeOrExit(final String args[]) {
        try {
            return new CmdLineOptions(args);
        } catch (final IllegalArgumentException ex) {
            final String msg = ex.getMessage();
            int exitCode = 0;
            if (!msg.equals("help")) {
                System.err.println(ex.getMessage());
                exitCode = -1;
            }
            CmdLineOptions.printUsage();
            System.exit(exitCode);
            return null;
        }
    }

    public CmdLineOptions(final String[] args) {
        int specifiedWidth = 1024;
        int specifiedHeight = 1024;
        int specifiedWorkers = 1;

        for (int curArg = 0; curArg < args.length; curArg++) {
            final String arg = args[curArg];

            if (arg.equalsIgnoreCase("-help") || arg.equalsIgnoreCase("-usage")) {
                throw new IllegalArgumentException("help");
            } else if (arg.equalsIgnoreCase("-workers") || arg.equalsIgnoreCase("-w")) {
                curArg++;
                if (curArg >= args.length) {
                    throw new IllegalArgumentException("Illegal " + arg + " argument with no corresponding number");
                }
                final String arg2 = args[curArg];
                try {
                    specifiedWorkers = Integer.parseInt(arg2);
                } catch (final Exception e) {
                    throw new IllegalArgumentException("workers argument is not parseable as an integer " + arg2);
                }
            } else if (arg.equalsIgnoreCase("-width")) {
                curArg++;
                if (curArg >= args.length) {
                    throw new IllegalArgumentException("Illegal " + arg + " argument with no corresponding number");
                }
                final String arg2 = args[curArg];
                try {
                    specifiedWidth = Integer.parseInt(arg2);
                } catch (final Exception e) {
                    throw new IllegalArgumentException("depth argument is not parseable as an integer " + arg2);
                }
            } else if (arg.equalsIgnoreCase("-height")) {
                curArg++;
                if (curArg >= args.length) {
                    throw new IllegalArgumentException("Illegal " + arg + " argument with no corresponding number");
                }
                final String arg2 = args[curArg];
                try {
                    specifiedHeight = Integer.parseInt(arg2);
                } catch (final Exception e) {
                    throw new IllegalArgumentException("spare places argument is not parseable as a long " + arg2);
                }
            } else {
                throw new IllegalArgumentException("Unknown argument " + arg);
            }
        }

        if (specifiedWidth <= 0) {
            throw new IllegalArgumentException("width argument must be positive, not " + specifiedWidth);
        }
        if (specifiedHeight <= 0) {
            throw new IllegalArgumentException("height argument must be positive, not " + specifiedWidth);
        }

        if (specifiedWorkers < 0) {
            throw new IllegalArgumentException("workers argument must be non-negative, not " + specifiedWorkers);
        }

        this.width = specifiedWidth;
        this.height = specifiedHeight;
        this.workers = specifiedWorkers;
    }

    /**
     * parses a string and returns the corresponding timespan in nanoseconds
     */
    public static long getTime(String timeString) {
        final long units;

        if (timeString.endsWith("ns")) {
            timeString = timeString.substring(0, timeString.length() - 2);
            units = 1;
        } else if (timeString.endsWith("ms")) {
            timeString = timeString.substring(0, timeString.length() - 2);
            units = 1000;
        } else if (timeString.endsWith("s")) {
            timeString = timeString.substring(0, timeString.length() - 1);
            units = 1000 * 1000;
        } else if (timeString.endsWith("m")) {
            timeString = timeString.substring(0, timeString.length() - 1);
            units = 1000 * 1000 * 60;
        } else if (timeString.endsWith("h")) {
            timeString = timeString.substring(0, timeString.length() - 1);
            units = 1000 * 1000 * 60 * 60;
        } else {
            units = 1;
        }
        return Long.parseLong(timeString) * units;
    }

    static void printUsage() {
        System.err.println("invoked as MarianiSilver ARGS where ARGS can be from");
        System.err.println("-help\t\tPrint this usage message and quit");

        System.err.println("-workers <INT>\t\tSet the the number of tasks used. If 0, uses the cluster default.");
        System.err.println("-width <INT>\t\tSet the width of the image");
        System.err.println("-height <INT>\t\tSet the height of the image");
    }
}
