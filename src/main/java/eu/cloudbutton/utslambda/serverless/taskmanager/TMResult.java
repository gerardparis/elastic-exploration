package eu.cloudbutton.utslambda.serverless.taskmanager;

import java.io.Serializable;

import eu.cloudbutton.utslambda.Bag;

public class TMResult implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private Bag bag;
    private long initTs;
    private long endTs;
    
    public TMResult(Bag bag, long initTs, long endTs) {
        super();
        this.bag = bag;
        this.initTs = initTs;
        this.endTs = endTs;
    }

    public Bag getBag() {
        return bag;
    }

    public long getInitTs() {
        return initTs;
    }

    public long getEndTs() {
        return endTs;
    }
    
    

}
