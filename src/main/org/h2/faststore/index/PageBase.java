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

package org.h2.faststore.index;

import org.h2.api.ErrorCode;
import org.h2.engine.Session;
import org.h2.faststore.ArrayObject;
import org.h2.faststore.sync.SXLatch;
import org.h2.faststore.type.FSRecord;
import org.h2.message.DbException;
import org.h2.result.SearchRow;
import org.h2.store.Data;

/**
 * TODO
 * 1. insert, delete, update
 * 2. split, merge
 * 3. redo/undo, rollback
 * 4. MVCC
 *
 * 1. array(record, record, record, ...)
 *
 * 2. array(record -> record -> record -> ...) + directory
 * more space(record pointer), less ops during DML, query might slow
 * PageBtree, rows[], offsets[],
 *
 */
public abstract class PageBase {

    static final int PAGE_SIZE = 4096;

    public static final int MAX_DIRECTORY_SIZE = 8;
    public static final int MIN_DIRECTORY_SIZE = 4;

    //TODO change it
    public static final int MAX_RECORD_SIZE = PAGE_SIZE >>> 1;

    private int pageSize;

    protected int entryCount;

    private long pageId;

    private boolean sm_bit;

    protected FSIndex index;

    protected long parentPageId;

    protected Data data;

    //TODO or something like this
    private long pageLSN;

    private SXLatch latch;

    //TODO need to write to disk
    //write end.offset & count each Directory
    protected Directory[] directories;
    protected int directoryCount;

    protected static class Directory extends ArrayObject {
        public int count;
        public FSRecord start;
        public FSRecord end;
//        private static final Directory[] EMPTY_ARRAY = {};

        public static Directory INSTANCE = new Directory(null, null, -1);

        public Directory(FSRecord start, FSRecord end, int count) {
            this.start = start;
            this.end = end;
            this.count = count;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Directory count:").append(count).append(" [");
            FSRecord r = start;
            while (r != end) {
                sb.append(r.toString()).append("; ");
                r = r.getNext();
            }
            sb.append(end.toString()).append("]");
            return sb.toString();
        }

        @Override
        protected ArrayObject[] createArray(int sz) {
            return new Directory[sz];
        }
    }

    public PageBase(long pageId, FSIndex index) {
        this.pageId = pageId;
        this.index = index;
        this.data = Data.create(null, new byte[PAGE_SIZE]);
        latch = new SXLatch(index.getName() + "-page " + pageId);
    }

    protected void checkDuplicate(FSRecord cmpRecord, boolean add) {
        if (add && index.getIndexType().isUnique()) {
            if (!index.containsNullAndAllowMultipleNull(cmpRecord)) {
                throw index.getDuplicateKeyException(cmpRecord.toString());
            }
        }
    }

    public FSRecord getMinMaxKey(boolean min) {
        if (isEmptyPage()) {
            throw DbException.get(ErrorCode.GENERAL_ERROR_1, "Leaf Page "
                    + getPageId() + " is empty");
        }

        return min ? directories[0].start : directories[directoryCount - 1].end;
    }

    // bigger  true for the next bigger row, false for the first row = target
    // compareKeys true for compare the row key, for delete
    //abstract int find(FSRecord compare, boolean bigger, boolean add, boolean compareInnerKey);

    //return the smallest one >= record
    int find(FSRecord cmpRecord, boolean add) {
        int l = 0, r = directoryCount - 1;
        while (l <= r) {
            int m = (l + r) >>> 1;
            FSRecord record = directories[m].end;
            int c = index.compareKeys(record, cmpRecord);
            if (c == 0) {
                checkDuplicate(cmpRecord, add);
                c = index.compareInnerKey(record, cmpRecord);
                if (c == 0) {
                    return m;
                }
            }

            // c != 0
            if (c > 0) {
                r = m - 1;
            } else {
                l = m + 1;
            }
        }

        return -(l + 1);
    }


    //fetch: return the smallest one >= cmpRecord. compareInnerKey
    //add:   duplicate check & smallest one > record in directory & compareInnerKey
    //del:   compareInnerKey
    protected FSRecord findRecord(FSRecord cmpRecord, boolean add) {
        int idx = find(cmpRecord, add);
        if (idx >= 0) {
            return directories[idx].end;
        }

        idx = -idx - 1;
        if (idx >= directoryCount) {
            return null;
        }
        FSRecord record = directories[idx].start;
        for (int i = 0; i < directories[idx].count - 1; i++) {
            int cmp = index.compareKeys(record, cmpRecord);
            if (cmp == 0) {
                checkDuplicate(cmpRecord, add);
                cmp = index.compareInnerKey(record, cmpRecord);
                if (cmp == 0) {
                    return record;
                }
            }

            //cmp != 0
            if (cmp > 0) {
                break;
            }
            record = record.getNext();
        }

        return record;
    }

    abstract public void splitPage(int splitPoint, FSLeafPage nextPage);

//    abstract public int addRow(FSRecord record);
//    abstract public FSRecord removeRow(FSRecord record);

    abstract public int getMemory();

    public long getPageId() {
        return pageId;
    }

    public void setPageId(long pageId) {
        this.pageId = pageId;
    }

    public long getPageLSN() {
        return pageLSN;
    }

    public void setPageLSN(long pageLSN) {
        this.pageLSN = pageLSN;
    }

    public void latch(Session session, boolean exclusive) {
        latch.latch(session, exclusive);
    }

    public void unlatch(Session session) {
        latch.unlatch(session);
    }

    public boolean isEmptyPage() {
        return entryCount == 0;
    }

    public int getEntryCount() {
        return entryCount;
    }

    public boolean getSMBit() {
        return sm_bit;
    }

    public void setSMBit(boolean sm_bit) {
        this.sm_bit = sm_bit;
    }

    public boolean isLeaf() {
        return false;
    }

    public void setParentPageId(long parentPageId) {
        this.parentPageId = parentPageId;
    }

    public long getParentPageId() {
        return parentPageId;
    }

}
