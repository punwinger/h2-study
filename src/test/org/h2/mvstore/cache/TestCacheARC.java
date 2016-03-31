package org.h2.mvstore.cache;

import org.h2.util.New;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Random;

/**
 * No warranty
 */
public class TestCacheARC {

    @Test
    public void testPut() {
        // c = 4
        CacheARC<String> c = new CacheARC<String>(10, 3);
        ////Case IV.B. if == false
        c.put(10, "abc");
        validCache(c, new long[]{10}, new long[]{}, new long[]{}, new long[]{});
        c.put(21, "adbc");
        validCache(c, new long[]{21,10}, new long[]{}, new long[]{}, new long[]{});
        c.put(12, "asbc");
        validCache(c, new long[]{12,21,10}, new long[]{}, new long[]{}, new long[]{});
        c.put(46, "abac");
        validCache(c, new long[]{46,12,21,10}, new long[]{}, new long[]{}, new long[]{});

        //Case IV.A. else
        c.put(23, "asbc");
        validCache(c, new long[]{23,46,12,21}, new long[]{}, new long[]{}, new long[]{});

        //Case I
        c.put(12, "asssssbc");
        validCache(c, new long[]{23,46,21}, new long[]{}, new long[]{12}, new long[]{});
        c.put(23, "asbc");
        validCache(c, new long[]{46,21}, new long[]{}, new long[]{23,12}, new long[]{});
        c.put(12, "dddddd");
        validCache(c, new long[]{46,21}, new long[]{}, new long[]{12,23}, new long[]{});
        c.put(23, "ddcx");
        validCache(c, new long[]{46,21}, new long[]{}, new long[]{23,12}, new long[]{});

        //Case IV.B.if.!= 2c
        c.put(231, "asbc");
        validCache(c, new long[]{231,46}, new long[]{21}, new long[]{23,12}, new long[]{});
        c.put(22, "asbc");
        validCache(c, new long[]{22,231}, new long[]{46,21}, new long[]{23,12}, new long[]{});

        //Case IV.A.if
        c.put(24, "dddd");
        validCache(c, new long[]{24,22}, new long[]{231,46}, new long[]{23,12}, new long[]{});

        //Case II
        //p == 1
        c.put(46, "aaaaaa");
        validCache(c, new long[]{24}, new long[]{22,231}, new long[]{46,23,12}, new long[]{});
        //p == 2
        //replace.else
        c.put(22, "ddd");
        validCache(c, new long[]{24}, new long[]{231}, new long[]{22,46,23}, new long[]{12});

        //Case III
        //p == 1
        //replace if
        c.put(12, "sasas");
        validCache(c, new long[]{}, new long[]{24,231}, new long[]{12,22,46,23}, new long[]{});
        c.put(13, "sasas");
        validCache(c, new long[]{13}, new long[]{24,231}, new long[]{12,22,46}, new long[]{23});
        c.put(14, "sasas");
        validCache(c, new long[]{14,13}, new long[]{24,231}, new long[]{12,22}, new long[]{46,23});
        //p == 2
        c.put(231, "dddda");
        validCache(c, new long[]{14,13}, new long[]{24,}, new long[]{231,12}, new long[]{22,46,23});
        //p == 3
        c.put(24, "dddda");
        validCache(c, new long[]{14,13}, new long[]{}, new long[]{24,231}, new long[]{12,22,46,23});

        //Case IV.B. == 2c
        c.put(17, "dddda");
        validCache(c, new long[]{17,14,13}, new long[]{}, new long[]{24}, new long[]{231,12,22,46});
        c.put(18, "dddda");
        validCache(c, new long[]{18,17,14,13}, new long[]{}, new long[]{}, new long[]{24,231,12,22});
        c.put(24, "dddda");
        validCache(c, new long[]{18,17,14}, new long[]{13}, new long[]{24}, new long[]{231,12,22});

        //Case II. B2/B1
        //p == 4
        c.put(13, "dddaawe");
        validCache(c, new long[]{18,17,14}, new long[]{}, new long[]{13}, new long[]{24,231,12,22});
    }

    @Test
    public void testGet() {
        CacheARC<String> c = getCache();
        Assert.assertTrue(c.get(13) != null);
        validCache(c, new long[]{13,14}, new long[]{24,231}, new long[]{12,22}, new long[]{46,23});

        Assert.assertTrue(c.get(24) == null);
        validCache(c, new long[]{13,14}, new long[]{24,231}, new long[]{12,22}, new long[]{46,23});

        Assert.assertTrue(c.get(22) != null);
        validCache(c, new long[]{13,14}, new long[]{24,231}, new long[]{22,12}, new long[]{46,23});

        Assert.assertTrue(c.get(23) == null);
        validCache(c, new long[]{13,14}, new long[]{24,231}, new long[]{22,12}, new long[]{23,46});

        c.clear();
        Assert.assertTrue(c.get(22) == null);
    }

    @Test
    public void testRemove() {
        CacheARC<String> c = getCache();
        validCache(c, new long[]{14,13}, new long[]{24,231}, new long[]{12,22}, new long[]{46,23});
        Assert.assertTrue(c.remove(14) != null);
        Assert.assertTrue(c.get(14) == null);
        validCache(c, new long[]{13}, new long[]{24,231}, new long[]{12,22}, new long[]{46,23});
        Assert.assertTrue(c.remove(13) != null);
        validCache(c, new long[]{}, new long[]{24,231}, new long[]{12,22}, new long[]{46,23});

        c.put(24, "asdf");
        validCache(c, new long[]{}, new long[]{231}, new long[]{24, 12, 22}, new long[]{46, 23});

        c.put(231, "asdf");
        validCache(c, new long[]{}, new long[]{}, new long[]{231,24, 12, 22}, new long[]{46, 23});

        Assert.assertTrue(c.remove(24) != null);
        validCache(c, new long[]{}, new long[]{}, new long[]{231, 12, 22}, new long[]{46, 23});
        Assert.assertTrue(c.remove(46) == null);
        validCache(c, new long[]{}, new long[]{}, new long[]{231, 12, 22}, new long[]{ 23});
        Assert.assertTrue(c.remove(231) != null);
        validCache(c, new long[]{}, new long[]{}, new long[]{12, 22}, new long[]{ 23});
        Assert.assertTrue(c.remove(12) != null);
        validCache(c, new long[]{}, new long[]{}, new long[]{ 22}, new long[]{ 23});
        Assert.assertTrue(c.remove(22) != null);
        validCache(c, new long[]{}, new long[]{}, new long[]{}, new long[]{ 23});
        Assert.assertTrue(c.remove(23) == null);
        validCache(c, new long[]{}, new long[]{}, new long[]{}, new long[]{});
        Assert.assertTrue(c.remove(22) == null);
        c.clear();
    }

    public CacheARC<String> getCache() {
        CacheARC<String> c = new CacheARC<String>(10, 3);
        c.put(10, "abc");
        validCache(c, new long[]{10}, new long[]{}, new long[]{}, new long[]{});
        c.put(21, "adbc");
        validCache(c, new long[]{21,10}, new long[]{}, new long[]{}, new long[]{});
        c.put(12, "asbc");
        validCache(c, new long[]{12,21,10}, new long[]{}, new long[]{}, new long[]{});
        c.put(46, "abac");
        validCache(c, new long[]{46,12,21,10}, new long[]{}, new long[]{}, new long[]{});
        c.put(23, "asbc");
        validCache(c, new long[]{23,46,12,21}, new long[]{}, new long[]{}, new long[]{});
        c.put(12, "asssssbc");
        validCache(c, new long[]{23,46,21}, new long[]{}, new long[]{12}, new long[]{});
        c.put(23, "asbc");
        validCache(c, new long[]{46,21}, new long[]{}, new long[]{23,12}, new long[]{});
        c.put(12, "dddddd");
        validCache(c, new long[]{46,21}, new long[]{}, new long[]{12,23}, new long[]{});
        c.put(23, "ddcx");
        validCache(c, new long[]{46,21}, new long[]{}, new long[]{23,12}, new long[]{});
        c.put(231, "asbc");
        validCache(c, new long[]{231,46}, new long[]{21}, new long[]{23,12}, new long[]{});
        c.put(22, "asbc");
        validCache(c, new long[]{22,231}, new long[]{46,21}, new long[]{23,12}, new long[]{});
        c.put(24, "dddd");
        validCache(c, new long[]{24,22}, new long[]{231,46}, new long[]{23,12}, new long[]{});
        c.put(46, "aaaaaa");
        validCache(c, new long[]{24}, new long[]{22,231}, new long[]{46,23,12}, new long[]{});
        c.put(22, "ddd");
        validCache(c, new long[]{24}, new long[]{231}, new long[]{22,46,23}, new long[]{12});
        c.put(12, "sasas");
        validCache(c, new long[]{}, new long[]{24,231}, new long[]{12,22,46,23}, new long[]{});
        c.put(13, "sasas");
        validCache(c, new long[]{13}, new long[]{24,231}, new long[]{12,22,46}, new long[]{23});
        c.put(14, "sasas");
        validCache(c, new long[]{14,13}, new long[]{24,231}, new long[]{12,22}, new long[]{46,23});

        return c;
    }



    private void validCache(CacheARC c, long[] t1, long[] b1, long[] t2, long[] b2) {
        CacheARC.Entry[][] lists = c.getList();
        int i = 0;
        //T1
        Assert.assertTrue(c.getSizeT1() == t1.length);
        for (i = 0; i < t1.length; i++) {
            CacheARC.Entry e = lists[0][i];
            Assert.assertTrue(e.value != null);
            Assert.assertTrue(e.key == t1[i]);
            Assert.assertTrue(e.isInT1());
        }
        if (i < lists[0].length) {
            Assert.assertTrue(lists[0][i] == null);
        }

        //B1
        Assert.assertTrue(c.getSizeB1() == b1.length);
        for (i = 0; i < b1.length; i++) {
            CacheARC.Entry e = lists[1][i];
            Assert.assertTrue(e.value == null);
            Assert.assertTrue(e.key == b1[i]);
            Assert.assertTrue(e.isInB1());
        }
        if (i < lists[0].length) {
            Assert.assertTrue(lists[1][i] == null);
        }

        //T2
        Assert.assertTrue(c.getSizeT2() == t2.length);
        for (i = 0; i < t2.length; i++) {
            CacheARC.Entry e = lists[2][i];
            Assert.assertTrue(e.value != null);
            Assert.assertTrue(e.key == t2[i]);
            Assert.assertTrue(e.isInT2());
        }
        if (i < lists[0].length) {
            Assert.assertTrue(lists[2][i] == null);
        }

        //B2
        Assert.assertTrue(c.getSizeB2() == b2.length);
        for (i = 0; i < b2.length; i++) {
            CacheARC.Entry e = lists[3][i];
            Assert.assertTrue(e.value == null);
            Assert.assertTrue(e.key == b2[i]);
            Assert.assertTrue(e.isInB2());
        }
        if (i < lists[0].length) {
            Assert.assertTrue(lists[3][i] == null);
        }
    }

    static class LimitLru extends LinkedHashMap<Integer, Integer> {
        private int maxSize;

        public LimitLru(int size) {
            super(size);
            this.maxSize = size;
        }

        @Override
        public Integer put(Integer key, Integer value) {
            if (size() == maxSize && !containsKey(key)) {
                return null;
            }
            return super.put(key, value);
        }

    }



//    Test 0, LIRS: 24.5% ARC: 39.04% LRU: 24.56%
//    Test 1, LIRS: 24.38% ARC: 38.98% LRU: 24.43%
//    Test 2, LIRS: 24.26% ARC: 38.65% LRU: 24.37%
//    Test 3, LIRS: 24.3% ARC: 38.75% LRU: 24.35%
//    Test 4, LIRS: 24.26% ARC: 38.57% LRU: 24.18%
    @Test
    public void testHitRatio() {
        boolean log = false;
        int size = 5000;
        Random r = new Random();
        for (int j = 0; j < 5; j++) {
            CacheLIRS<Integer> test = new CacheLIRS<Integer>(size,1,0);
            CacheARC<Integer> test2 = new CacheARC<>(size,1);
            HashMap<Integer, Integer> good = New.hashMap();
            LimitLru test3 = new LimitLru(size);
            int hit = 0, hit2 = 0, hit3= 0;
            int total = 239457;
            for (int i = 0; i < total; i++) {
                // key distribution affect the hit ratio
                int key = r.nextInt(size * 2);
                int value = r.nextInt();
                switch (r.nextInt(2)) {
                    case 0:
                        if (log) {
                            System.out.println(i + " put " + key + " " + value);
                        }
                        good.put(key, value);
                        test.put(key, value);
                        test2.put(key,value);
                        test3.put(key, value);
                        break;
                    case 1:
                        if (log) {
                            System.out.println(i + " get " + key);
                        }
                        Integer a = good.get(key);
                        Integer b = test.get(key);
                        Integer b2 = test2.get(key);
                        Integer b3 = test3.get(key);

                        if (a == null) {
                            Assert.assertNull(b);
                            Assert.assertNull(b2);
                            break;
                        }

                        if (b != null) {
                            Assert.assertEquals(a, b);
                            hit++;
                        } else {
                            test.put(key, a);
                        }
                        if (b2 != null) {
                            Assert.assertEquals(a, b2);
                            hit2++;
                        } else {
                            test2.put(key, a);
                        }

                        if (b3 != null) {
                            Assert.assertEquals(a, b3);
                            hit3++;
                        } else {
                            test3.put(key, a);
                        }
                        break;
                    case 2:
                        if (log) {
                            System.out.println(i + " remove " + key);
                        }
                        good.remove(key);
                        test.remove(key);
                        test2.remove(key);
                        test3.remove(key);
                        break;
                }

            }

            System.out.println("Test " + j + ", LIRS: " + hitRatio(hit, total) + " ARC: " + hitRatio(hit2, total)
            + " LRU: " + hitRatio(hit3, total));
        }
    }

    private String hitRatio(double hit, int total) {
        return  Double.toString(Math.round(hit / total * 10000) / 100.0) + "%";
    }


}
