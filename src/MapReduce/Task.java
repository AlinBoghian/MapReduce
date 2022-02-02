package MapReduce;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Task implements Runnable {
    public int nrTasks;
    public AtomicInteger tasksDone;
    public Semaphore sem;


    /**
     * method called by run() that defines what a worker does during a processing stage
     */
    abstract void work();

    @Override
    public void run() {
        work();
        tasksDone.incrementAndGet();
        if(tasksDone.get() == nrTasks){
            sem.release();
        }
    }
}
