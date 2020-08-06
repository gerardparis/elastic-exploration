package eu.cloudbutton.utslambda.serverless.taskmanager;

import java.util.UUID;

public class TaskStats {
    private UUID bagId;
    private UUID parentBagId;
    private long initTime;
    private long duration;

    public TaskStats(UUID bagId, UUID parentBagId, long initTime, long duration) {
        this.bagId=bagId;
        this.parentBagId=parentBagId;
        this.initTime = initTime;
        this.duration = duration;
    }

    public UUID getParentBagId() {
        return parentBagId;
    }

    public void setParentBagId(UUID parentBagId) {
        this.parentBagId = parentBagId;
    }

    public UUID getBagId() {
        return bagId;
    }

    public void setBagId(UUID bagId) {
        this.bagId = bagId;
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
        return bagId.toString() + "[" + parentBagId + "] " + initTime + " (" + duration + ")";
    }
}
