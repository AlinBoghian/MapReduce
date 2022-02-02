package MapReduce;

import MapReduce.Collectors.KeyValueCollector;
import MapReduce.Collectors.KeyValueListsCollector;

/**
 *
 * Task that carries out the combination of key value pairs into one single structure that maps key to list of values
 */
public class ShuffleTask<Key,Value> extends Task {

    public KeyValueCollector<Key,Value> collector;
    public KeyValueListsCollector<Key,Value> listsCollector;
    @Override
    public void work() {
        for(var entry : collector.getKeyValuePairs()){
            listsCollector.put(entry.key(), entry.value());
        }
    }
}
