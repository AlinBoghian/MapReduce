package MapReduce;

import java.util.Objects;

/**
 *
 * Class that encapsulates data sent from the shuffle phase to the reduce workers
 */
public final class ReduceResults<Key, Value> {
    private final Key key;
    private final Value value;

    public ReduceResults(Key key, Value value) {
        this.key = key;
        this.value = value;
    }

    public Key key() {
        return key;
    }

    public Value value() {
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (ReduceResults) obj;
        return Objects.equals(this.key, that.key) &&
                Objects.equals(this.value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    @Override
    public String toString() {
        return "ReduceResults[" +
                "key=" + key + ", " +
                "value=" + value + ']';
    }

}
