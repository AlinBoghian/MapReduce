package MapReduce.Collectors;

import MapReduce.Pair;

import java.util.ArrayList;
import java.util.List;

public class KeyValueCollector<Key, Value> {
    List<Pair<Key, Value>> keyValuePairs;

    public KeyValueCollector() {
        this.keyValuePairs = new ArrayList<>();
    }

    public void collect(Key key,Value value){
        keyValuePairs.add(new Pair<>(key,value));
    }

    public List<Pair<Key, Value>> getKeyValuePairs(){
        return keyValuePairs;
    }
}
