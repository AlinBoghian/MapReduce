package MapReduce;

import MapReduce.Collectors.KeyValueCollector;
import MapReduce.Collectors.KeyValueListsCollector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class that coordinates the workers that apply MapReduce and intermediary operations
 *
 * @param <Key> Type of the keys used in key-value pairs
 * @param <Value1> type of values produced by Map
 * @param <Value2> type of values produced by Reduce
 */
public class MapReducer<Key,Value1,Value2> {

    public Mapper<Key,Value1> mapper;
    public Reducer<Key,Value1,Value2> reducer;
    public int nrWorkers;
    public List<String> files;
    public Integer chunk_size;
    public String delim;

    /**
     *
     * @return lists of results containing a key and value each
     */
    public List<ReduceResults<Key,Value2>> mapReduce(){
        LinkedBlockingQueue<MapArgs> mapArgs = new LinkedBlockingQueue<>();
        ExecutorService execs = Executors.newFixedThreadPool(nrWorkers);
        Semaphore sem = new Semaphore(0);

        AtomicInteger tasksDone = new AtomicInteger(0);

        //split phase
        int filenr = files.size();
        for(var file : files){
            var splitter = new SplitTask();
            splitter.filename = file;
            splitter.chunksize = chunk_size;
            splitter.mapArgs = mapArgs;
            splitter.sem = sem;
            splitter.nrTasks = filenr;
            splitter.tasksDone = tasksDone;
            execs.submit(splitter);
        }
        //wait for tasks to finish
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        tasksDone.set(0);
        int mapTasksnr = mapArgs.size();


        ArrayList<KeyValueCollector<Key,Value1>> collectors = new ArrayList<>(mapTasksnr);

        //map phase
        sem = new Semaphore(0);
        for(var args : mapArgs){
            var mapWorker = new MapTask<Key,Value1>();
            mapWorker.nrTasks = mapTasksnr;
            mapWorker.mapper = mapper;
            mapWorker.tasksDone = tasksDone;
            mapWorker.filedata = args;
            KeyValueCollector<Key,Value1> collector = new KeyValueCollector<>();
            mapWorker.collector = collector;
            mapWorker.delim = delim;
            collectors.add(collector);
            mapWorker.sem = sem;
            execs.submit(mapWorker);
        }

        //wait for tasks to finish
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //Shuffle phase
        KeyValueListsCollector<Key,Value1> listcollector = new KeyValueListsCollector<>();
        tasksDone.set(0);
        sem = new Semaphore(0);
        for(var collector : collectors){
            var shuffler = new ShuffleTask<Key,Value1>();
            shuffler.sem = sem;
            shuffler.nrTasks = collectors.size();
            shuffler.listsCollector = listcollector;
            shuffler.collector = collector;
            shuffler.tasksDone = tasksDone;
            execs.submit(shuffler);
        }

        //wait for tasks to finish
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //Reduce phase
        tasksDone.set(0);
        LinkedBlockingQueue<ReduceResults<Key,Value2>> results = new LinkedBlockingQueue<>();
        var listSet = listcollector.getKeyValueLists();
        int listsnr = listSet.size();
        sem = new Semaphore(0);
        for(var entry : listSet){
            ReduceTask<Key,Value1,Value2> reduceTask = new ReduceTask<>();
            reduceTask.key = entry.getKey();
            reduceTask.values =  entry.getValue();
            reduceTask.results = results;
            reduceTask.sem = sem;
            reduceTask.nrTasks = listsnr;
            reduceTask.tasksDone = tasksDone;
            reduceTask.reducer = reducer;
            execs.submit(reduceTask);
        }
        //wait for tasks to finish
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //stop executors service and return the list containing results
        execs.shutdown();
        LinkedList<ReduceResults<Key,Value2>> finalresults = new LinkedList<>();
        results.drainTo(finalresults);
        return finalresults;
    }
}
