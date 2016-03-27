package org.h2.mvstore.cache;

/**
 * Adaptive Replacement Cache.
 * See
 * ARC: A SELF-TUNING, LOW OVERHEAD REPLACEMENT CACHE.
 * Nimrod Megiddo and Dharmendra S. Modha
 */
public class CacheARC<V> {

    private Entry<V> list1;

    //mid1 refer to the last entry of T1
    private Entry<V> mid1;

    private Entry<V> list2;

    //mid2 refer to the last entry of T2
    private Entry<V> mid2;

    private int sizeT1;

    private int sizeB1;

    private int sizeT2;

    private int sizeB2;

    private int sizeCache;

    private int p;

    //Map entries.
    //Better HashMap for
    // 1. key is long, not need to create object
    // 2. better customized hash function see answered by Thomas Mueller
    // http://stackoverflow.com/questions/664014/what-integer-hash-function-are-good-that-accepts-an-integer-hash-key
    private Entry<V>[] entries;



    private static class Entry<V> {
        private static final int NOT_IN_LIST = -1;
        private static final int IN_T1 = 0;
        private static final int IN_B1 = 1;
        private static final int IN_T2 = 2;
        private static final int IN_B2 = 3;

        long key;

        V value;

        Entry<V> prev;

        Entry<V> next;

        Entry<V> mapNext;

        int posType = NOT_IN_LIST;
    }


    public CacheARC(long maxMemory, int averageMemory) {
        int len = (int) (maxMemory / averageMemory);
        //round up to power of 2
        sizeCache = Integer.highestOneBit((len - 1) << 1);

        //overflow problem?
        entries = new Entry[2 * sizeCache];

        list1 = new Entry<V>();
        list1.prev = list1.next = list1;
        mid1 = list1;

        list2 = new Entry<V>();
        list2.prev = list2.next = list2;
        mid2 = list2;
    }


    public V put(long key, V value) {
        int hash = getHash(key);

        Entry<V> e = find(key, hash);

        V old = null;

        if (e == null) {
            if (sizeT1 + sizeB1 == sizeCache) {
                if (sizeT1 < sizeCache) {

                } else {

                }
            } else {

            }

        } else {
            if (e.posType == Entry.IN_B1 || e.posType == Entry.IN_B2) {
                adaption(e.posType == Entry.IN_B1);
                replaceCache(e);
            }

            // move to the head of T2
            removeFromList(e);
            addToMRU(e, list2);
            sizeAdjust(e.posType, Entry.IN_T2);
            e.posType = Entry.IN_T2;

            old = e.value;
            e.value = value;
        }

        return old;
    }

    private void sizeAdjust(int oldPos, int newPos) {
        switch (oldPos) {
            case Entry.IN_B1:
                sizeB1--;
                break;
            case Entry.IN_B2:
                sizeB2--;
                break;
            case Entry.IN_T1:
                sizeT1--;
                break;
            case Entry.IN_T2:
                sizeT2--;
                break;
            default:
                    break;
        }

        switch (newPos) {
            case Entry.IN_B1:
                sizeB1++;
                break;
            case Entry.IN_B2:
                sizeB2++;
                break;
            case Entry.IN_T1:
                sizeT1++;
                break;
            case Entry.IN_T2:
                sizeT2++;
                break;
            default:
                break;
        }
    }

    private void addToMRU(Entry<V> e, Entry<V> list) {
        e.next = list.next;
        e.prev = list;
        list.next.prev = e;
        list.next = e;
    }

    private void replaceCache(Entry<V> e) {
        if (sizeT1 > 0 && (sizeT1 > p || (sizeT1 == p && e.posType == Entry.IN_B2))) {
            mid1.value = null;
            mid1 = mid1.prev;
        } else {
            mid2.value = null;
            mid2 = mid2.prev;
        }
    }

    private void adaption(boolean in_b1) {
        if (in_b1) {
            p += sizeB1 >= sizeB2 ? 1 : sizeB2 / sizeB1;
        } else {
            p -= sizeB2 >= sizeB1 ? 1 : sizeB1 / sizeB2;
        }
    }

    private void removeFromList(Entry<V> e) {
        mid1 = mid1 == e ? e.prev : mid1;
        mid2 = mid2 == e ? e.prev : mid2;
        e.prev.next = e.next;
        e.next.prev = e.prev;
    }

    private Entry<V> find(long key, int hash) {
        int idx = hash & (entries.length - 1);

        Entry<V> res = entries[idx];
        while (res != null && res.key != key) {
            res = res.mapNext;
        }

        return res;
    }

    static int getHash(long key) {
        //copy from CacheLongKeyLIRS
        //http://stackoverflow.com/questions/664014/what-integer-hash-function-are-good-that-accepts-an-integer-hash-key
        int hash = (int) ((key >>> 32) ^ key);
        // a supplemental secondary hash function
        // to protect against hash codes that don't differ much
        hash = ((hash >>> 16) ^ hash) * 0x45d9f3b;
        hash = ((hash >>> 16) ^ hash) * 0x45d9f3b;
        hash = (hash >>> 16) ^ hash;
        return hash;
    }

}
