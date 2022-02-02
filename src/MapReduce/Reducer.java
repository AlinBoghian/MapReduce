package MapReduce;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public interface Reducer<Key,Value1,Value2>{
    /**
     *
     * @param key the key that we are reducing the values of
     * @param values associated list of values
     * @param results entity that collects our result
     */
    void reduce(Key key, Collection<Value1> values, Collection<ReduceResults<Key,Value2>> results);
}
