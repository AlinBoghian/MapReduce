package MapReduce;

import java.util.Objects;

/**
 * Map that encapsulates data that is passed from split phase to map workers
 */
public final class MapArgs {
    private final String filename;
    private final int offset;
    private final int chunksize;

    public MapArgs(String filename, int offset, int chunksize) {
        this.filename = filename;
        this.offset = offset;
        this.chunksize = chunksize;
    }

    public String filename() {
        return filename;
    }

    public int offset() {
        return offset;
    }

    public int chunksize() {
        return chunksize;
    }

}
