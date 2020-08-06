package eu.cloudbutton.utslambda;

public class CmdLineOptions {
    public final int depth;
    final int warmupDepth;
    public final int parallelism;
    final long spares;
    final long[] killTimes;

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
        int specifiedDepth = 13;
        int specifiedWarmupDepth = -2;
        int specifiedWorkers = 1;
        long specifiedSpares = 0L;
        final int maxPlaces = 1;
        // for each place, stores a time to suicide
        // 0 means that it will not suicide
        this.killTimes = new long[maxPlaces];

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
            } else if (arg.equalsIgnoreCase("-depth") || arg.equalsIgnoreCase("-d")) {
                curArg++;
                if (curArg >= args.length) {
                    throw new IllegalArgumentException("Illegal " + arg + " argument with no corresponding depth");
                }
                final String arg2 = args[curArg];
                try {
                    specifiedDepth = Integer.parseInt(arg2);
                } catch (final Exception e) {
                    throw new IllegalArgumentException("depth argument is not parseable as an integer " + arg2);
                }
            } else if (arg.equalsIgnoreCase("-spares") || arg.equalsIgnoreCase("-s")) {
                curArg++;
                if (curArg >= args.length) {
                    throw new IllegalArgumentException("Illegal " + arg + " argument with no corresponding depth");
                }
                final String arg2 = args[curArg];
                try {
                    specifiedSpares = Long.parseLong(arg2);
                } catch (final Exception e) {
                    throw new IllegalArgumentException("spare places argument is not parseable as a long " + arg2);
                }
            } else if (arg.equalsIgnoreCase("-warmupDepth") || arg.equalsIgnoreCase("-warmup")
                    || arg.equalsIgnoreCase("-wd")) {
                curArg++;
                if (curArg >= args.length) {
                    throw new IllegalArgumentException("Illegal " + arg + " argument with no corresponding depth");
                }
                final String arg2 = args[curArg];
                try {
                    specifiedWarmupDepth = Integer.parseInt(arg2);
                } catch (final Exception e) {
                    throw new IllegalArgumentException("warmupDepth argument is not parseable as an integer " + arg2);
                }
            } else if (arg.equalsIgnoreCase("-kill") || arg.equalsIgnoreCase("-killAfter")
                    || arg.equalsIgnoreCase("-k")) {
                curArg++;
                if (curArg >= args.length) {
                    throw new IllegalArgumentException(
                            "Illegal " + arg + " argument with no corresponding place:timespan argument");
                }
                final String arg2 = args[curArg];
                final String[] sp = arg2.split(":");
                if (sp.length != 2) {
                    throw new IllegalArgumentException(
                            "Malformed " + arg + " argument: '" + arg2 + "' does not have exactly one colon");
                }

                final String timeString = sp[1];
                long timeToKill;
                try {
                    timeToKill = getTime(timeString);
                } catch (final Exception e) {
                    throw new IllegalArgumentException("Malformed " + arg + " argument.  The second part of '" + arg2
                            + "' is not parseable as a long.  Note that only ns, ms, s, and m, and h are allowed as suffixes");
                }

                // allow comma separated list of places which can have ranges
                final String[] toKillRangeList = sp[0].split(",");
                for (String toKillRange : toKillRangeList) {
                    final String[] range = toKillRange.split("-");
                    if (range.length == 0) {
                        // allow and ignore stray commas
                        continue;
                    } else if (range.length > 2) {
                        throw new IllegalArgumentException("Malformed " + arg + " argument.  The first part of '" + arg2
                                + "' has an invalid range specification: '" + toKillRange + "' (too many - symbols)");
                    }

                    int firstPlaceToKill = -1;
                    int lastPlaceToKill = -1;

                    try {
                        firstPlaceToKill = Integer.parseInt(range[0]);
                    } catch (final Exception e) {
                        throw new IllegalArgumentException("Malformed " + arg + " argument.  The first part of '" + arg2
                                + "' has a place specifier '" + range[0] + "'that is not parseable as a place list");
                    }

                    if (range.length == 2) {
                        try {
                            lastPlaceToKill = Integer.parseInt(range[1]);
                        } catch (final Exception e) {
                            throw new IllegalArgumentException("Malformed " + arg + " argument.  The first part of '"
                                    + arg2 + "' has a place specifier '" + range[1]
                                    + "'that is not parseable as a place list");
                        }
                    } else {
                        lastPlaceToKill = firstPlaceToKill;
                    }
                    if (firstPlaceToKill < 0 || firstPlaceToKill > lastPlaceToKill) {
                        throw new IllegalArgumentException("Malformed " + arg + " argument.  The first part of '" + arg2
                                + "' has a range specifier '" + toKillRange + "' that is not a valid range.");
                    }

                    if (firstPlaceToKill == 0) {
                        throw new IllegalArgumentException("The " + arg
                                + " argument.  Requested that place 0 die (as part of) '" + arg2 + "', specifically '"
                                + toKillRange + "'.  We don't currently support killing place 0");
                    }

                    // allow and ignore places that are too large
                    final long cappedLastPlaceToKill = Math.min(lastPlaceToKill, maxPlaces - 1);
                    for (int pl = firstPlaceToKill; pl <= cappedLastPlaceToKill; pl++)
                        killTimes[pl] = timeToKill;
                }

            } else {
                try {
                    specifiedDepth = Integer.parseInt(arg);
                } catch (final Exception e) {
                    throw new IllegalArgumentException("Unknown argument " + arg);
                }
            }
        }

        if (specifiedDepth <= 0) {
            throw new IllegalArgumentException("depth argument must be positive, not " + specifiedDepth);
        }

        if (specifiedWorkers < 0) {
            throw new IllegalArgumentException("workers argument must be non-negative, not " + specifiedWorkers);
        }

        this.depth = specifiedDepth;
        this.warmupDepth = specifiedWarmupDepth < 0 ? specifiedDepth + specifiedWarmupDepth : specifiedWarmupDepth;
        this.parallelism = specifiedWorkers;
        this.spares = specifiedSpares;
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
        System.err.println("invoked as SparkUTS ARGS where ARGS can be from");
        System.err.println("-help\t\tPrint this usage message and quit");

        System.err.println("-workers <INT>\t\tSet the the number of tasks used. If 0, uses the cluster default.");
        System.err.println("-depth <INT>\t\tSet the depth to be used");
        System.err.println("-d <INT>\t\tSet the depth to be used");
        // System.err.println("-s <INT>\t\tSet the spare places to be used");
        System.err.println("<INT>\t\tSet the depth to be used");
        System.err.println(
                "-warmupDepth <INT>\t\tSet the depth to be used for warmup.  Negative value is relative to depth.  0 omits the warmup. The default is -2");

        // System.err.println("-kill <place>:<timespan>\t\tTells the place to kill
        // itself after the allotted timespan (after any warmup)");
        // System.err.println("\t\t <timespan> can be specified, using an optional
        // suffix, in nanoseconds (ns, default), milliseconds (ms), seconds(s),
        // minutes(m), or hours(h)");

    }
}
