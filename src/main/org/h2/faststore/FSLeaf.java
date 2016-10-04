/*
 * Copyright (c) 2016. Pun.W <punyj177 at gmail dot com>
 *
 * Everyone is permitted to copy and distribute verbatim or modified copies of this license
 * document, and changing it is allowed as long as the name is changed.
 *
 * DO WHAT THE FUCK YOU WANT TO PUBLIC LICENSE TERMS AND CONDITIONS FOR COPYING,
 * DISTRIBUTION AND MODIFICATION
 *
 * 0. You just DO WHAT THE FUCK YOU WANT TO.
 */

package org.h2.faststore;

import org.h2.faststore.FSDirectoryLeaf.Directory;
import org.h2.faststore.FSDirectoryLeaf.SpacePos;
import org.h2.faststore.type.FSRecord;
import org.h2.message.DbException;
import org.h2.result.SearchRow;
import org.h2.store.Data;
import org.h2.table.IndexColumn;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueInt;
import org.h2.value.ValueString;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Random;

abstract class FSLeaf {
    static final int COPY_THRESHOLD = 4;
    static final int DELETE_THRESHOLD = COPY_THRESHOLD + COPY_THRESHOLD / 2;
    static final int PAGE_SIZE = 4096;

    protected IndexColumn[] indexColumns;
    protected int[] columnIds;
    protected CompareMode compareMode;
    protected int start;
    protected int entryCount;
    protected Data data;

    //存储全部数据，所以需要这个
    protected int columnCount;

    public FSLeaf(IndexColumn[] indexColumns, int[] columnIds,
                  CompareMode compareMode, int columnCount) {
        this.indexColumns = indexColumns;
        this.columnIds = columnIds;
        this.compareMode = compareMode;
        this.data = Data.create(null, new byte[PAGE_SIZE]);
        this.columnCount = columnCount;
    }

    public int entryCount() {
        return entryCount;
    }

    protected static int[] insertInArray(int[] old, int oldSize, int pos, int x) {
        int[] result;
        if (old != null && old.length > oldSize) {
            result = old;
        } else {
            result = new int[oldSize + 1 + COPY_THRESHOLD];
            if (pos > 0 && old != null) {
                System.arraycopy(old, 0, result, 0, pos);
            }
        }
        if (old != null && oldSize - pos > 0) {
            System.arraycopy(old, pos, result, pos + 1, oldSize - pos);
        }
        result[pos] = x;
        return result;
    }

    protected static int[] removeFromArray(int[] old, int oldSize, int pos) {
        int[] result;
        if (old.length <= oldSize + DELETE_THRESHOLD) {
            result = old;
        } else {
            result = new int[oldSize + COPY_THRESHOLD - 1];
            if (pos > 0) {
                System.arraycopy(old, 0, result, 0, pos);
            }
        }

        if (oldSize - pos - 1 > 0) {
            System.arraycopy(old, pos + 1, result, pos, oldSize - pos - 1);
        }

        return result;
    }

    protected static FSRecord[] insertInArray(FSRecord[] old, int oldSize, int pos, FSRecord x) {
        FSRecord[] result;
        if (old != null && old.length > oldSize) {
            result = old;
        } else {
            result = new FSRecord[oldSize + 1 + COPY_THRESHOLD];
            if (pos > 0 && old != null) {
                System.arraycopy(old, 0, result, 0, pos);
            }
        }
        if (old != null && oldSize - pos > 0) {
            System.arraycopy(old, pos, result, pos + 1, oldSize - pos);
        }
        result[pos] = x;
        return result;
    }

    protected static FSRecord[] removeFromArray(FSRecord[] old, int oldSize, int pos) {
        FSRecord[] result;
        if (old.length <= oldSize + DELETE_THRESHOLD) {
            result = old;
        } else {
            result = new FSRecord[oldSize + COPY_THRESHOLD - 1];
            if (pos > 0) {
                System.arraycopy(old, 0, result, 0, pos);
            }
        }

        if (oldSize - pos - 1 > 0) {
            System.arraycopy(old, pos + 1, result, pos, oldSize - pos - 1);
        }

        return result;
    }



    protected int getRowSize(SearchRow row) {
        int size = 0;
        for (int i = 0; i < columnCount; i++) {
            size += data.getValueLen(row.getValue(i));
        }
        return size;
    }

    protected int compareInnerKey(FSRecord a, FSRecord b) {
        long aKey = a.getKey();
        long bKey = b.getKey();
        return aKey == bKey ? 0 : (aKey > bKey ? 1 : -1);
    }


    protected int compareRecord(FSRecord l, FSRecord r) {
        return l.compare(r, columnIds, indexColumns, compareMode);
    }


    abstract public FSRecord findRecord(FSRecord cmpRecord, boolean compareInnerKey);

    abstract public int addRow(FSRecord record);

    abstract public FSRecord removeRow(FSRecord record);

    private static int initKey;
    private static final String SOURCE = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
    private static Random random = new Random();


//    TYPE D
//    Add:66765 Del:66798 spend:134 find:331 spend:135
//    TYPE S
//    Add:66780 Del:66823 spend:99 find:292 spend:99

    public static <T> T[] createArray(T[] old) {
        return  (T[]) Array.newInstance(
                old.getClass().getComponentType(), 1 + COPY_THRESHOLD);
    }

    public static SpacePos[] createArray2(SpacePos[] old) {
        return new SpacePos[1 + COPY_THRESHOLD];
    }

    public static Directory[] createArray2(Directory[] old) {
        return new Directory[1 + COPY_THRESHOLD];
    }

    public static TestA[] createArray2(TestA[] old) {
        return new TestA[1 + COPY_THRESHOLD];
    }

    public static ArrayObject[] createArray3(ArrayObject[] old, ArrayObject a) {
        return  a.createArray(1 + COPY_THRESHOLD);
    }

    interface ArrayObject {
        ArrayObject[] createArray(int sz);
    }


//    public static <T> T[] insert(T[] old, int oldSize, int pos, T x) {
//        T[] result;
//        if (old.length > oldSize) {
//            result = old;
//        } else {
//            // according to a test, this is as fast as "new Row[..]"
//            result = (T[]) Array.newInstance(
//                    old.getClass().getComponentType(), oldSize + 1 + COPY_THRESHOLD);
//            if (pos > 0) {
//                System.arraycopy(old, 0, result, 0, pos);
//            }
//        }
//        if (oldSize - pos > 0) {
//            System.arraycopy(old, pos, result, pos + 1, oldSize - pos);
//        }
//        result[pos] = x;
//        return result;
//    }

    private static class TestA implements ArrayObject {
        private int a;
        private int b;
        private int c;
        private int d;
        private String e;

        @Override
        public ArrayObject[] createArray(int sz) {
            return new TestA[sz];
        }
    }

    public static void main(String[] args) {

        SpacePos[] empty_array = {};
        Directory[] empty_array_2 = {};
        TestA[] empty_array_3 = {};
        TestA t = new TestA();

        long beg = 0;

        if (args[0].equals("aa")) {
            beg = System.nanoTime();
            for (int i = 0; i < 9999; i++) {
                SpacePos[] a = createArray(empty_array);
            }
        } else if (args[0].equals("ab")) {
            beg = System.nanoTime();
            for (int i = 0; i < 9999; i++) {
                SpacePos[] a = createArray2(empty_array);
            }
        } else if (args[0].equals("ba")) {
            beg = System.nanoTime();
            for (int i = 0; i < 9999; i++) {
                Directory[] a = createArray(empty_array_2);
            }
        } else if (args[0].equals("bb")) {
            beg = System.nanoTime();
            for (int i = 0; i < 9999; i++) {
                Directory[] a = createArray2(empty_array_2);
            }
        } else if (args[0].equals("ca")) {
            beg = System.nanoTime();
            for (int i = 0; i < 99999; i++) {
                TestA[] a = createArray(empty_array_3);
            }
        } else if (args[0].equals("cb")) {
            beg = System.nanoTime();
            for (int i = 0; i < 99999; i++) {
                TestA[] a = createArray2(empty_array_3);
            }
        } else if (args[0].equals("cc")) {
            beg = System.nanoTime();
            for (int i = 0; i < 99999; i++) {
                //TestA[] a = (TestA[]) createArray3(empty_array_3, t);
                TestA[] a = (TestA[]) t.createArray(1 + COPY_THRESHOLD);
            }
        }


        long dur = System.nanoTime() - beg;
        System.out.println("spend time:" + dur);




//        //performance test.
//        IndexColumn[] idxColumn = new IndexColumn[] {new IndexColumn()};
//        int[] columnIds = new int[] {0};
//        CompareMode mode = CompareMode.getInstance(null, 0, true);
//
//        String type = args[0];
//        FSLeaf leaf = null;
//        if ("S".equals(type)) {
//            leaf = new FSSimpleLeaf(idxColumn, columnIds, mode, 2);
//        } else {
//            leaf = new FSDirectoryLeaf(idxColumn, columnIds, mode, 2);
//        }
//
//        //record len 20~30. max 204~136
//        // add, remove
//        // find
//        int recordMax = 200;
//        FSRecord[] recs = new FSRecord[recordMax];
//        for (int i = 0; i < recordMax; i++) {
//            recs[i] = createRecord();
//
//            if (i < 100) {
//                leaf.addRow(recs[i]);
//            }
//        }
//
//        long begin = System.currentTimeMillis();
//        int add = 0;
//        int del = 0;
//
//        for (int i = 0; i < 10000; i++) {
//            for (int j = 0; j < 10; j++) {
//                int r = random.nextInt(recordMax);
//                int e = leaf.entryCount;
//                leaf.addRow(recs[r]);
//                if (e < leaf.entryCount) {
//                    add++;
//                }
//            }
//
//            for (int j = 0; j < 20; j++) {
//                int r = random.nextInt(recordMax);
//                int e = leaf.entryCount;
//                leaf.removeRow(recs[r]);
//
//                if (e > leaf.entryCount) {
//                    del++;
//                }
//
//                recs[r].setOffset(-1);
//                recs[r].setNext(null);
//            }
//        }
//
//        long end1 = System.currentTimeMillis();
//
//        int find = 0;
//        for (int i = 0; i < 1000; i++) {
//            int r = random.nextInt(recordMax);
//            if (leaf.findRecord(recs[r], true) != null) {
//                find++;
//            }
//        }
//
//        long end2 = System.currentTimeMillis();
//        System.out.println("TYPE " + type);
//        System.out.println("Add:" + add + " Del:" + del
//                + " spend:" + (end1 - begin) + " find:" + find + " spend:" + (end2 - begin));

    }

    private static FSRecord createRecord() {
        FSRecord record = new FSRecord(2);
        int k = initKey++;
        record.setKey(k);
        Value v = ValueInt.get(k * 10);
        record.setValue(0, v);
        v = ValueString.get(generateRandomString(random.nextInt(20)) + "0000000000");
        record.setValue(1, v);
        return record;
    }

    private static String generateRandomString(int len) {
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            sb.append(SOURCE.charAt(random.nextInt(SOURCE.length())));
        }
        return sb.toString();
    }
}
