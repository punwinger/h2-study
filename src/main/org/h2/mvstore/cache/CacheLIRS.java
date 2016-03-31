package org.h2.mvstore.cache;

import org.h2.mvstore.DataUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * LIRS implement without segment.
 * for performance test
 */
public class CacheLIRS<V> {

    /**
     * The number of (hot, cold, and non-resident) entries in the map.
     */
    int mapSize;

    /**
     * The size of the LIRS queue for resident cold entries.
     */
    int queueSize;

    /**
     * The size of the LIRS queue for non-resident cold entries.
     */
    int queue2Size;

    /**
     * The map array. The size is always a power of 2.
     */
    Entry<V>[] entries;

    /**
     * The currently used memory.
     */
    long usedMemory;

    /**
     * How many other item are to be moved to the top of the stack before
     * the current item is moved.
     */
    private final int stackMoveDistance;

    /**
     * The maximum memory this cache should use.
     */
    private long maxMemory;

    /**
     * The average memory used by one entry.
     */
    private int averageMemory;

    /**
     * The bit mask that is applied to the key hash code to get the index in
     * the map array. The mask is the length of the array minus one.
     */
    private int mask;

    /**
     * The LIRS stack size.
     */
    private int stackSize;

    /**
     * The stack of recently referenced elements. This includes all hot
     * entries, the recently referenced cold entries, and all non-resident
     * cold entries.
     * <p/>
     * There is always at least one entry: the head entry.
     */
    private Entry<V> stack;

    /**
     * The queue of resident cold entries.
     * <p/>
     * There is always at least one entry: the head entry.
     */
    private Entry<V> queue;

    /**
     * The queue of non-resident cold entries.
     * <p/>
     * There is always at least one entry: the head entry.
     */
    private Entry<V> queue2;

    /**
     * The number of times any item was moved to the top of the stack.
     */
    private int stackMoveCounter;

    /**
     * Create a new cache.
     *
     * @param maxMemory         the maximum memory to use
     * @param averageMemory     the average memory usage of an object
     * @param stackMoveDistance the number of other entries to be moved to
     *                          the top of the stack before moving an entry to the top
     */
    CacheLIRS(long maxMemory, int averageMemory, int stackMoveDistance) {
        setMaxMemory(maxMemory);
        setAverageMemory(averageMemory);
        this.stackMoveDistance = stackMoveDistance;
        clear();
    }

    public void clear() {

        // calculate the size of the map array
        // assume a fill factor of at most 80%
        long maxLen = (long) (maxMemory / averageMemory / 0.75);
        // the size needs to be a power of 2
        long l = 8;
        while (l < maxLen) {
            l += l;
        }
        // the array size is at most 2^31 elements
        int len = (int) Math.min(1L << 31, l);
        // the bit mask has all bits set
        mask = len - 1;

        // initialize the stack and queue heads
        stack = new Entry<V>();
        stack.stackPrev = stack.stackNext = stack;
        queue = new Entry<V>();
        queue.queuePrev = queue.queueNext = queue;
        queue2 = new Entry<V>();
        queue2.queuePrev = queue2.queueNext = queue2;

        // first set to null - avoiding out of memory
        entries = null;
        @SuppressWarnings("unchecked")
        Entry<V>[] e = new Entry[len];
        entries = e;

        mapSize = 0;
        usedMemory = 0;
        stackSize = queueSize = queue2Size = 0;
    }

    /**
     * Get the memory used for the given key.
     *
     * @param key  the key (may not be null)
     * @param hash the hash
     * @return the memory, or 0 if there is no resident entry
     */
    int getMemory(long key, int hash) {
        Entry<V> e = find(key, hash);
        return e == null ? 0 : e.memory;
    }

    public V get(long key) {
        int hash = getHash(key);
        Entry<V> e = find(key, hash);
        if (e == null) {
            // the entry was not found
            return null;
        }
        V value = e.value;
        if (value == null) {
            // it was a non-resident entry
            return null;
        }
        if (e.isHot()) {
            if (e != stack.stackNext) {
                if (stackMoveDistance == 0 ||
                        stackMoveCounter - e.topMove > stackMoveDistance) {
                    access(key, hash);
                }
            }
        } else {
            access(key, hash);
        }
        return value;
    }

    /**
     * Access an item, moving the entry to the top of the stack or front of
     * the queue if found.
     *
     * @param key the key
     */
    private synchronized void access(long key, int hash) {
        Entry<V> e = find(key, hash);
        if (e == null || e.value == null) {
            return;
        }
        if (e.isHot()) {
            if (e != stack.stackNext) {
                if (stackMoveDistance == 0 ||
                        stackMoveCounter - e.topMove > stackMoveDistance) {
                    // move a hot entry to the top of the stack
                    // unless it is already there
                    boolean wasEnd = e == stack.stackPrev;
                    removeFromStack(e);
                    if (wasEnd) {
                        // if moving the last entry, the last entry
                        // could now be cold, which is not allowed
                        pruneStack();
                    }
                    addToStack(e);
                }
            }
        } else {
            removeFromQueue(e);
            if (e.stackNext != null) {
                // resident cold entries become hot
                // if they are on the stack
                removeFromStack(e);
                // which means a hot entry needs to become cold
                // (this entry is cold, that means there is at least one
                // more entry in the stack, which must be hot)
                convertOldestHotToCold();
            } else {
                // cold entries that are not on the stack
                // move to the front of the queue
                addToQueue(queue, e);
            }
            // in any case, the cold entry is moved to the top of the stack
            addToStack(e);
        }
    }

    public V put(long key, V value) {
        if (value == null) {
            throw DataUtils.newIllegalArgumentException(
                    "The value may not be null");
        }

        int hash = getHash(key);
        int memory = averageMemory;
        V old;
        Entry<V> e = find(key, hash);
        if (e == null) {
            old = null;
        } else {
            old = e.value;
            remove(key);
        }
        e = new Entry<V>();
        e.key = key;
        e.value = value;
        e.memory = memory;
        int index = hash & mask;
        e.mapNext = entries[index];
        entries[index] = e;
        usedMemory += memory;
        if (usedMemory > maxMemory && mapSize > 0) {
            // an old entry needs to be removed
            evict(e);
        }
        mapSize++;
        // added entries are always added to the stack
        addToStack(e);
        return old;
    }

    public V remove(long key) {
        int hash = getHash(key);
        int index = hash & mask;
        Entry<V> e = entries[index];
        if (e == null) {
            return null;
        }
        V old;
        if (e.key == key) {
            old = e.value;
            entries[index] = e.mapNext;
        } else {
            Entry<V> last;
            do {
                last = e;
                e = e.mapNext;
                if (e == null) {
                    return null;
                }
            } while (e.key != key);
            old = e.value;
            last.mapNext = e.mapNext;
        }
        mapSize--;
        usedMemory -= e.memory;
        if (e.stackNext != null) {
            removeFromStack(e);
        }
        if (e.isHot()) {
            // when removing a hot entry, the newest cold entry gets hot,
            // so the number of hot entries does not change
            e = queue.queueNext;
            if (e != queue) {
                removeFromQueue(e);
                if (e.stackNext == null) {
                    addToStackBottom(e);
                }
            }
        } else {
            removeFromQueue(e);
        }
        pruneStack();
        return old;
    }

    /**
     * Evict cold entries (resident and non-resident) until the memory limit
     * is reached. The new entry is added as a cold entry, except if it is
     * the only entry.
     *
     * @param newCold a new cold entry
     */
    private void evict(Entry<V> newCold) {
        // ensure there are not too many hot entries: right shift of 5 is
        // division by 32, that means if there are only 1/32 (3.125%) or
        // less cold entries, a hot entry needs to become cold
        while (queueSize <= (mapSize >>> 5) && stackSize > 0) {
            convertOldestHotToCold();
        }
        if (stackSize > 0) {
            // the new cold entry is at the top of the queue
            addToQueue(queue, newCold);
        }
        // the oldest resident cold entries become non-resident
        // but at least one cold entry (the new one) must stay
        while (usedMemory > maxMemory && queueSize > 1) {
            Entry<V> e = queue.queuePrev;
            usedMemory -= e.memory;
            removeFromQueue(e);
            e.value = null;
            e.memory = 0;
            addToQueue(queue2, e);
            // the size of the non-resident-cold entries needs to be limited
            while (queue2Size + queue2Size > stackSize) {
                e = queue2.queuePrev;
                remove(e.key);
            }
        }
    }

    private void convertOldestHotToCold() {
        // the last entry of the stack is known to be hot
        Entry<V> last = stack.stackPrev;
        if (last == stack) {
            // never remove the stack head itself (this would mean the
            // internal structure of the cache is corrupt)
            throw new IllegalStateException();
        }
        // remove from stack - which is done anyway in the stack pruning,
        // but we can do it here as well
        removeFromStack(last);
        // adding an entry to the queue will make it cold
        addToQueue(queue, last);
        pruneStack();
    }

    /**
     * Ensure the last entry of the stack is cold.
     */
    private void pruneStack() {
        while (true) {
            Entry<V> last = stack.stackPrev;
            // must stop at a hot entry or the stack head,
            // but the stack head itself is also hot, so we
            // don't have to test it
            if (last.isHot()) {
                break;
            }
            // the cold entry is still in the queue
            removeFromStack(last);
        }
    }

    /**
     * Get the hash code for the given key. The hash code is
     * further enhanced to spread the values more evenly.
     *
     * @param key the key
     * @return the hash code
     */
    static int getHash(long key) {
        int hash = (int) ((key >>> 32) ^ key);
        // a supplemental secondary hash function
        // to protect against hash codes that don't differ much
        hash = ((hash >>> 16) ^ hash) * 0x45d9f3b;
        hash = ((hash >>> 16) ^ hash) * 0x45d9f3b;
        hash = (hash >>> 16) ^ hash;
        return hash;
    }

    /**
     * Try to find an entry in the map.
     *
     * @param key  the key
     * @param hash the hash
     * @return the entry (might be a non-resident)
     */
    Entry<V> find(long key, int hash) {
        int index = hash & mask;
        Entry<V> e = entries[index];
        while (e != null && e.key != key) {
            e = e.mapNext;
        }
        return e;
    }

    private void addToStack(Entry<V> e) {
        e.stackPrev = stack;
        e.stackNext = stack.stackNext;
        e.stackNext.stackPrev = e;
        stack.stackNext = e;
        stackSize++;
        e.topMove = stackMoveCounter++;
    }

    private void addToStackBottom(Entry<V> e) {
        e.stackNext = stack;
        e.stackPrev = stack.stackPrev;
        e.stackPrev.stackNext = e;
        stack.stackPrev = e;
        stackSize++;
    }

    /**
     * Remove the entry from the stack. The head itself must not be removed.
     *
     * @param e the entry
     */
    private void removeFromStack(Entry<V> e) {
        e.stackPrev.stackNext = e.stackNext;
        e.stackNext.stackPrev = e.stackPrev;
        e.stackPrev = e.stackNext = null;
        stackSize--;
    }

    private void addToQueue(Entry<V> q, Entry<V> e) {
        e.queuePrev = q;
        e.queueNext = q.queueNext;
        e.queueNext.queuePrev = e;
        q.queueNext = e;
        if (e.value != null) {
            queueSize++;
        } else {
            queue2Size++;
        }
    }

    private void removeFromQueue(Entry<V> e) {
        e.queuePrev.queueNext = e.queueNext;
        e.queueNext.queuePrev = e.queuePrev;
        e.queuePrev = e.queueNext = null;
        if (e.value != null) {
            queueSize--;
        } else {
            queue2Size--;
        }
    }

    /**
     * Get the list of keys. This method allows to read the internal state
     * of the cache.
     *
     * @param cold        if true, only keys for the cold entries are returned
     * @param nonResident true for non-resident entries
     * @return the key list
     */
    synchronized List<Long> keys(boolean cold, boolean nonResident) {
        ArrayList<Long> keys = new ArrayList<Long>();
        if (cold) {
            Entry<V> start = nonResident ? queue2 : queue;
            for (Entry<V> e = start.queueNext; e != start;
                 e = e.queueNext) {
                keys.add(e.key);
            }
        } else {
            for (Entry<V> e = stack.stackNext; e != stack;
                 e = e.stackNext) {
                keys.add(e.key);
            }
        }
        return keys;
    }

    public boolean containsKey(long key) {
        int hash = getHash(key);
        Entry<V> e = find(key, hash);
        return e != null && e.value != null;
    }

    /**
     * Get the set of keys for resident entries.
     *
     * @return the set of keys
     */
    public Set<Long> keySet() {
        HashSet<Long> set = new HashSet<Long>();
        for (Entry<V> e = stack.stackNext; e != stack; e = e.stackNext) {
            set.add(e.key);
        }
        for (Entry<V> e = queue.queueNext; e != queue; e = e.queueNext) {
            set.add(e.key);
        }
        return set;
    }

    /**
     * Set the maximum memory this cache should use. This will not
     * immediately cause entries to get removed however; it will only change
     * the limit. To resize the internal array, call the clear method.
     *
     * @param maxMemory the maximum size (1 or larger)
     */
    void setMaxMemory(long maxMemory) {
        this.maxMemory = maxMemory;
    }

    /**
     * Set the average memory used per entry. It is used to calculate the
     * length of the internal array.
     *
     * @param averageMemory the average memory used (1 or larger)
     */
    void setAverageMemory(int averageMemory) {
        this.averageMemory = averageMemory;
    }


    /**
     * A cache entry. Each entry is either hot (low inter-reference recency;
     * LIR), cold (high inter-reference recency; HIR), or non-resident-cold. Hot
     * entries are in the stack only. Cold entries are in the queue, and may be
     * in the stack. Non-resident-cold entries have their value set to null and
     * are in the stack and in the non-resident queue.
     *
     * @param <V> the value type
     */
    private static class Entry<V> {

        /**
         * The key.
         */
        long key;

        /**
         * The value. Set to null for non-resident-cold entries.
         */
        V value;

        /**
         * The estimated memory used.
         */
        int memory;

        /**
         * When the item was last moved to the top of the stack.
         */
        int topMove;

        /**
         * The next entry in the stack.
         */
        Entry<V> stackNext;

        /**
         * The previous entry in the stack.
         */
        Entry<V> stackPrev;

        /**
         * The next entry in the queue (either the resident queue or the
         * non-resident queue).
         */
        Entry<V> queueNext;

        /**
         * The previous entry in the queue.
         */
        Entry<V> queuePrev;

        /**
         * The next entry in the map
         */
        Entry<V> mapNext;

        /**
         * Whether this entry is hot. Cold entries are in one of the two queues.
         *
         * @return whether the entry is hot
         */
        boolean isHot() {
            return queueNext == null;
        }

    }
}
