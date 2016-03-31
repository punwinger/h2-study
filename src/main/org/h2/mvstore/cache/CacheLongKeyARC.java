package org.h2.mvstore.cache;

import org.h2.mvstore.DataUtils;

/**
 * Adaptive Replacement Cache
 * use segment to improve concurrency
 *
 */
public class CacheLongKeyARC<V> {
    private final Segment<V>[] segments;

    private final int segmentCount;
    private final int segmentShift;
    private final int segmentMask;

    private long maxMemory;
    private int averageMemory;

    public CacheLongKeyARC(int maxEntries) {
        this(maxEntries, 1, 16);
    }


    public CacheLongKeyARC(long maxMemory, int averageMemory,
                            int segmentCount) {
        DataUtils.checkArgument(
                Integer.bitCount(segmentCount) == 1,
                "The segment count must be a power of 2, is {0}", segmentCount);
        this.maxMemory = maxMemory;
        this.averageMemory = averageMemory;
        this.segmentCount = segmentCount;
        this.segmentMask = segmentCount - 1;
        segments = new Segment[segmentCount];
        clear();

        this.segmentShift = Integer.numberOfTrailingZeros(
                segments[0].entries.length);
    }

    public void clear() {
        long max = Math.max(1, maxMemory / segmentCount);
        for (int i = 0; i < segmentCount; i++) {
            segments[i] = new Segment<V>(
                    max, averageMemory);
        }
    }

    public V put(long key, V value) {
        int hash = getHash(key);
        return getSegment(hash).put(key, value, hash);
    }

    public V get(long key) {
        int hash = getHash(key);
        return getSegment(hash).get(key, hash);
    }

    public V remove(long key) {
        int hash = getHash(key);
        return getSegment(hash).remove(key, hash);
    }


    private Segment<V> getSegment(int hash) {
        int segmentIndex = (hash >>> segmentShift) & segmentMask;
        return segments[segmentIndex];
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

        boolean isInT1() {
            return posType == IN_T1;
        }

        boolean isInB1() {
            return posType == IN_B1;
        }

        boolean isInT2() {
            return posType == IN_T2;
        }

        boolean isInB2() {
            return posType == IN_B2;
        }
    }

    private static class Segment<V> {
        //size is [0, sizeCache]
        private Entry<V> list1;

        //mid1 refer to the last entry of T1
        private Entry<V> mid1;

        //size is [0, 2 * sizeCache]
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
        //Better Implement for
        // 1. key is long, not need to create object
        // 2. better customized hash function. see answered by Thomas Mueller
        // http://stackoverflow.com/questions/664014/what-integer-hash-function-are-good-that-accepts-an-integer-hash-key
        private Entry<V>[] entries;


        public Segment(long maxMemory, int averageMemory) {
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

        public V get(long key, int hash) {
            Entry<V> e = find(key, hash);
            if (e == null) {
                return null;
            }

            //move to the relative list head, IN_B1 -> B1, IN_T2 -> T2 etc
            removeFromList(e);
            if (e.posType == Entry.IN_B1) {
                addToMRU(e, mid1);
            } else if (e.posType == Entry.IN_B2) {
                addToMRU(e, mid2);
            } else if (e.posType == Entry.IN_T1) {
                addToMRU(e, list1);
            } else {
                addToMRU(e, list2);
            }

            return e.value;
        }

        public V remove(long key, int hash) {
            Entry<V> e = find(key, hash);
            if (e == null) {
                return null;
            }

            V oldVal = e.value;
            e.value = null;

            removeFromList(e);

            //remove from entries
            int idx = hash & (entries.length - 1);
            if (entries[idx] == e) {
                entries[idx] = e.mapNext;
            } else {
                Entry<V> pre = entries[idx];
                while (pre.mapNext != e) {
                    pre = pre.mapNext;
                }
                pre.mapNext = e.mapNext;
            }

            sizeAdjust(e.posType, Entry.NOT_IN_LIST);


            return oldVal;
        }

        public V put(long key, V value, int hash) {
            Entry<V> e = find(key, hash);

            V old = null;

            if (e == null) {
                e = new Entry<V>();
                e.key = key;
                e.value = value;
                int idx = hash & (entries.length - 1);
                e.mapNext = entries[idx];
                entries[idx] = e;

                if (sizeT1 + sizeB1 == sizeCache) {
                    if (sizeT1 < sizeCache) {
                        replaceCache(e);
                    }

                    Entry<V> replace = list1.prev;
                    replace.value = null;
                    removeFromList(replace);
                    sizeAdjust(replace.posType, Entry.NOT_IN_LIST);
                    replace.posType = Entry.NOT_IN_LIST;
                } else {
                    //sizeT1 + sizeB1 < sizeCache
                    if (sizeT1 + sizeT2 + sizeB1 + sizeB2 >= sizeCache) {
                        if (sizeT1 + sizeT2 + sizeB1 + sizeB2 == 2 * sizeCache) {
                            //delete LRU page in B2
                            Entry<V> replace = list2.prev;
                            removeFromList(replace);
                            sizeAdjust(replace.posType, Entry.NOT_IN_LIST);
                            replace.posType = Entry.NOT_IN_LIST;
                        }

                        replaceCache(e);
                    }
                }

                //move to the head of T1
                addToMRU(e, list1);
                sizeAdjust(e.posType, Entry.IN_T1);
                e.posType = Entry.IN_T1;

            } else {
                if (e.posType == Entry.IN_B1 || e.posType == Entry.IN_B2) {
                    adaption(e.posType == Entry.IN_B1);
                    replaceCache(e);
                }

                removeFromList(e);
                // move to the head of T2
                addToMRU(e, list2);
                sizeAdjust(e.posType, Entry.IN_T2);
                e.posType = Entry.IN_T2;

                old = e.value;
                e.value = value;
            }

            return old;
        }


        public void clear() {
            entries = new Entry[2 * sizeCache];

            list1 = new Entry<V>();
            list1.prev = list1.next = list1;
            mid1 = list1;

            list2 = new Entry<V>();
            list2.prev = list2.next = list2;
            mid2 = list2;

            sizeT1 = sizeT2 = sizeB1 = sizeB2 = p = 0;
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

            if (list == list1 && mid1 == list1) {
                mid1 = list1.next;
            } else if (list == list2 && mid2 == list2) {
                mid2 = list2.next;
            }
        }

        private void replaceCache(Entry<V> e) {
            //because remove will delete any entry in T1 or T2
            //so here must check if cache is full or not
            if (sizeT1 + sizeT2 < sizeCache) {
                return;
            }

            if (sizeT1 > 0 && (sizeT1 > p || (sizeT1 == p && e.posType == Entry.IN_B2))) {
                Entry<V> m = mid1;
                m.value = null;
                mid1 = mid1.prev;

                sizeAdjust(m.posType, Entry.IN_B1);
                m.posType = Entry.IN_B1;
            } else {
                Entry<V> m = mid2;
                m.value = null;
                mid2.value = null;
                mid2 = mid2.prev;

                sizeAdjust(m.posType, Entry.IN_B2);
                m.posType = Entry.IN_B2;
            }
        }

        private void adaption(boolean in_b1) {
            if (in_b1) {
                p += sizeB1 >= sizeB2 ? 1 : sizeB2 / sizeB1;
                p = Math.min(p, sizeCache);
            } else {
                p -= sizeB2 >= sizeB1 ? 1 : sizeB1 / sizeB2;
                p = Math.max(p, 0);
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


        public int getSizeT1() {
            return sizeT1;
        }

        public int getSizeB1() {
            return sizeB1;
        }

        public int getSizeT2() {
            return sizeT2;
        }

        public int getSizeB2() {
            return sizeB2;
        }
    }
}
