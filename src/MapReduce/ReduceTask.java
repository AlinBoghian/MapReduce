package MapReduce;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class ReduceTask<Key, Value1,Value2> extends Task {
    public Key key;
    public Collection<Value1> values;
    public LinkedBlockingQueue<ReduceResults<Key,Value2>> results;
    public Reducer<Key, Value1,Value2> reducer;
    @Override
    public void work() {
        reducer.reduce(key,values,results);
    }
}
