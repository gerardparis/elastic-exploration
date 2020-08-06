package eu.cloudbutton.mandelbrot.serverless;

public class TaskStats {
    private long initTime;
    private long duration;
    private ExecType execType;

    public TaskStats(long initTime, long duration) {
        this(initTime, duration, ExecType.REMOTE);
    }

    public TaskStats(long initTime, long duration, ExecType execType) {
        this.initTime = initTime;
        this.duration = duration;
        this.execType = execType;
    }

    public long getInitTime() {
        return initTime;
    }

    public void setInitTime(long initTime) {
        this.initTime = initTime;
    }

    public long getEndTime() {
        return initTime+duration;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public String toString(){
        return initTime + " (" + duration + ")";
    }

    public ExecType getExecType() {
        return execType;
    }
}
