package MapReduce.Collectors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

public class KeyValueListsCollector<Key,Value> {

    public ConcurrentHashMap<Key, LinkedBlockingQueue<Value>> map;

    public KeyValueListsCollector() {
        map = new ConcurrentHashMap<>();
    }

    public void put(Key key, Value value){
        try {
            map.computeIfAbsent(key, k -> new LinkedBlockingQueue<>());
            map.get(key).put(value);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public Set<Map.Entry<Key,LinkedBlockingQueue<Value>>> getKeyValueLists(){
        return map.entrySet();
    }
}
