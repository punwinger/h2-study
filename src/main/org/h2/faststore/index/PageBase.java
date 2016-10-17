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

    public static final int MAX_DIRECTORY_NUM = 8;
    public static final int MIN_DIRECTORY_NUM = 4;

    public static final int INVALID_PAGE_ID = -1;

    //TODO change it
    public static final int MAX_RECORD_SIZE = PAGE_SIZE >>> 1;

    protected int entryCount;

    protected FSIndex index;

    protected long parentPageId = INVALID_PAGE_ID;

    protected int pageSize;

    protected Data data;
    //TODO need to write to disk
    //write end.offset & count each Directory
    protected Directory[] directories;
    protected int directoryCount;

    protected SpaceManager spaceManager;

    //TODO or something like this
    private long pageLSN;

    private SXLatch latch;

    private long pageId;

    private boolean sm_bit;

    protected int maxDirectoryCount;
    protected int minDirectoryCount;

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

    public PageBase(long pageId, FSIndex index, int pageSize, int minDirectoryCount,
                    int maxDirectoryCount) {
        this.pageId = pageId;
        this.index = index;
        this.data = Data.create(null, new byte[pageSize]);
        latch = new SXLatch(index.getName() + "-page " + pageId);
        this.pageSize = pageSize;
        this.minDirectoryCount = minDirectoryCount;
        this.maxDirectoryCount = maxDirectoryCount;
        spaceManager = createSpaceManager();
    }

//    protected void checkDuplicate(FSRecord cmpRecord, boolean add) {
//        if (add && index.getIndexType().isUnique()) {
//            if (!index.containsNullAndAllowMultipleNull(cmpRecord)) {
//                throw index.getDuplicateKeyException(cmpRecord.toString());
//            }
//        }
//    }

    public SpaceManager createSpaceManager() {
        return new SpaceManager(pageSize);
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
            int c = index.compareAllKeys(record, cmpRecord, add);
//            int c = index.compareKeys(record, cmpRecord);
//            if (c == 0) {
//                checkDuplicate(cmpRecord, add);
//                c = index.compareInnerKey(record, cmpRecord);
//                if (c == 0) {
//                    return m;
//                }
//            }

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
            int cmp = index.compareAllKeys(record, cmpRecord, add);
//            int cmp = index.compareKeys(record, cmpRecord);
//            if (cmp == 0) {
//                checkDuplicate(cmpRecord, add);
//                cmp = index.compareInnerKey(record, cmpRecord);
//                if (cmp == 0) {
//                    return record;
//                }
//            }

            //cmp != 0
            if (cmp > 0) {
                break;
            }
            record = record.getNext();
        }

        return record;
    }

    // return pos. if > 0 then need split at pos. if <= 0 then allocate at pos.
    public int checkNeedSplitIfAdd(FSRecord record) {
        int recordSize;
        if (isLeaf()) {
            recordSize = index.getRecordSize(data, record);
            if (recordSize >= MAX_RECORD_SIZE) {
                //record over flow, need overflow page.
                //TODO add overflow page id size?
                recordSize = index.getRecordIndexSize(data, record);
            }
        } else {
            //TODO add childPageID size
            recordSize = index.getRecordIndexSize(data, record);
        }

        int allocatePos = spaceManager.tryAllocate(recordSize);
        if (allocatePos < 0) {
            if (entryCount <= 1) {
                //shouldn't be here because I limit the size of primary key.
                DbException.throwInternalError(
                        "entryCount = " + entryCount + " while check need split");
            }

            int idx = find(record, true);
            if (idx >= 0) {
                //duplicate insert
                throw index.getDuplicateKeyException(record.toString());
            }

            if (entryCount < 5) {
                return entryCount >>> 1;
            } else {
                idx = -idx - 1;
                int insertPos = 0;
                for (int i = 0; i < idx; i++) {
                    insertPos += directories[i].count;
                }
                //I don't want to compare so let the insertPos be the mid of directories[idx].
                insertPos += directories[idx].count >>> 1;
                int third = entryCount / 3;

                return insertPos < third ? third :
                        (insertPos >= third * 2 ? third * 2 : insertPos);
            }
        }

        return -allocatePos;
    }

    protected void reset() {
        spaceManager = createSpaceManager();
        //read all record
        directories = null;
        directoryCount = 0;
        entryCount = 0;
    }

    public int addRow(FSRecord record, int allocatePos) {
        boolean overflow = false;
        int recordSize;
        if (isLeaf()) {
            recordSize = index.getRecordSize(data, record);
            if (recordSize >= MAX_RECORD_SIZE) {
                //record over flow, need overflow page.
                //TODO add overflow page id size?
                recordSize = index.getRecordIndexSize(data, record);
                overflow = true;
            }
        } else {
            //TODO add childPageID size
            recordSize = index.getRecordIndexSize(data, record);
        }
        if (allocatePos < 0) {
            DbException.throwInternalError(
                    "Not enough space while add row " + record.toString());
        }

        if (entryCount == 0) {
            Directory dir = new Directory(record, record, 1);
            directories = (Directory[]) Directory.INSTANCE.insertInArray(
                    directories, directoryCount, 0, dir);
            directoryCount++;
            entryCount++;
            int offset = spaceManager.allocate(allocatePos, recordSize);
            if (offset < 0) {
                DbException.throwInternalError("allocate error. " + toString());
            }
            record.setOffset(offset);
            record.setNext(null);
            return -1;
        }

        //find insertPoint of directories
        int insertPoint, insertPosInDir = 0;
        insertPoint = find(record, true);
        if (insertPoint >= 0) {
            //duplicate insert
            throw index.getDuplicateKeyException(record.toString());
        }
        insertPoint = -insertPoint - 1;
        Directory dir = null;
        if (insertPoint >= directoryCount) {
            //record > directories[directoryCount - 1].end
            dir = directories[directoryCount - 1];
            insertPoint = directoryCount - 1;
//            insertPosInDir = dir.count;
            dir.end.setNext(record);
            dir.end = record;
            record.setNext(null);
        } else {
            //insert into directories
            dir = directories[insertPoint];
            FSRecord cur = dir.start;
            FSRecord prev = insertPoint > 0 ? directories[insertPoint - 1].end : null;

            boolean hasInsert = false;
            for (insertPosInDir = 0; insertPosInDir < dir.count; ++insertPosInDir) {
                int cmp = index.compareAllKeys(cur, record, true);
//                int cmp = index.compareKeys(cur, record);
//                if (cmp == 0) {
//                    checkDuplicate(record, true);
//                    cmp = index.compareInnerKey(cur, record);
//                    if (cmp == 0) {
//                        throw index.getDuplicateKeyException(record.toString());
//                    }
//                }

                if (cmp > 0) {
                    record.setNext(cur);
                    if (prev != null) {
                        prev.setNext(record);
                    }
                    if (insertPosInDir == 0) {
                        dir.start = record;
                    }
                    hasInsert = true;
                    break;
                }
                prev = cur;
                cur = cur.getNext();
            }

            if (!hasInsert) {
                DbException.throwInternalError("Directory cannot find pos to insert. insert:"
                        + record + " dir:" + dir + " detail:" + toString());
            }

        }

        if (++dir.count > maxDirectoryCount) {
            //split directories
            //this directory count -> 4. newDir count -> 5
            int splitPoint = dir.count >>> 1;

            FSRecord newEnd = dir.start;
            for (int i = 1; i < splitPoint; i++) {
                newEnd = newEnd.getNext();
            }

            Directory newDir = new Directory(newEnd.getNext(), dir.end, dir.count - splitPoint);
            dir.end = newEnd;
            dir.count = splitPoint;
            directories = (Directory[]) Directory.INSTANCE.insertInArray(
                    directories, directoryCount, insertPoint + 1, newDir);
            directoryCount++;
        }

        int offset = spaceManager.allocate(allocatePos, recordSize);
        if (offset < 0) {
            DbException.throwInternalError("allocate error. " + toString());
        }

        if (overflow) {
            //TODO handle overflow
            DbException.throwInternalError("handle overflow not implement.");
        }

        record.setOffset(offset);
        entryCount++;
        return -1;
    }

    //TODO use cursor to delete
    //this is find then delete
    //return:
    // 1.null if last record in directory not change or not find
    // 2.record if delete last record in directory
    // 3.newLast if last record in directory change
    public FSRecord removeRow(FSRecord record) {
        if (entryCount <= 0) {
            DbException.throwInternalError();
        }

        FSRecord del = null, newLast = null;
        int dirPos = find(record, false);
        if (dirPos >= 0) {
            Directory dir = directories[dirPos];
            del = dir.end;
            newLast = removeRecordInDir(dirPos);
            if (newLast == null) {
                newLast = record;
            }
        } else {
            dirPos = -dirPos - 1;
            if (dirPos >= directoryCount) {
                // not found
                return null;
            }

            Directory dir = directories[dirPos];
            FSRecord cur = dir.start;
            FSRecord prev = dirPos > 0 ? directories[dirPos - 1].end : null;
            for (int i = 0; i < dir.count - 1; i++) {
                int cmp = index.compareKeys(cur, record);
                if (cmp == 0) {
                    cmp = index.compareInnerKey(cur, record);
                    if (cmp == 0) {
                        if (prev != null) {
                            prev.setNext(cur.getNext());
                        }
                        if (dir.start == cur) {
                            dir.start = cur.getNext();
                        }
                        del = cur;
                        dir.count--;
                        entryCount--;
                        break;
                    }
                }

                if (cmp > 0) {
                    //not found
                    return null;
                }
                prev = cur;
                cur = cur.getNext();
            }
        }

        deallocateRecord(del);
        tryRebalanceDirectory(dirPos);

        return newLast;
    }

    protected void removeDirectoryRecord(int dirPos) {
        if (dirPos < 0 || dirPos >= directoryCount) {
            DbException.throwInternalError("invalid dirPos " +
                    dirPos + " directoryCount:" + directoryCount);
        }

        Directory dir = directories[dirPos];
        FSRecord del = dir.end;
        removeRecordInDir(dirPos);
        deallocateRecord(del);
        tryRebalanceDirectory(dirPos);
    }

    private FSRecord removeRecordInDir(int dirPos) {
        Directory dir = directories[dirPos];
        FSRecord newLast = null, del = dir.end;
        boolean isLastDirectory = dirPos == directoryCount - 1;
        if (dir.count == 1) {
            //remove the directory
            directories = (Directory[]) Directory.INSTANCE.removeFromArray(
                    directories, directoryCount, dirPos);
            directoryCount--;

            if (dirPos > 0) {
                //adjust the prev
                FSRecord lastEnd = directories[dirPos - 1].end;
                FSRecord newNext = dirPos < directoryCount ? directories[dirPos].start : null;
                lastEnd.setNext(newNext);
            }
        } else {
            FSRecord newEnd = dir.start;
            for (int i = 1; i < dir.count - 1; i++) {
                newEnd = newEnd.getNext();
            }
            newEnd.setNext(del.getNext());
            dir.end = newEnd;
            dir.count--;
        }

        if (--entryCount > 0 && isLastDirectory) {
            newLast = directories[directoryCount - 1].end;
        }
//        else if (entryCount == 0) {
//            //empty entry
//            newLast = record;
//        }

        return newLast;
    }

    private void deallocateRecord(FSRecord del) {
        if (del != null) {
            //TODO handle over flow
            spaceManager.deallocate(del.getOffset(), index.getRecordSize(data, del));
        }
    }

    private void tryRebalanceDirectory(int changedDirPos) {
        if (directoryCount > 1 && changedDirPos < directoryCount
                && directories[changedDirPos].count < minDirectoryCount) {
            Directory dir = directories[changedDirPos];
            if (changedDirPos < directoryCount - 1) {
                //rebalance with changedDirPos + 1
                Directory rightDir = directories[changedDirPos + 1];
                if (rightDir.count + dir.count >= (minDirectoryCount << 1)) {
                    //move records from rightDir to dir
                    FSRecord newEnd = dir.end;
                    for (int i = dir.count; i < minDirectoryCount; i++) {
                        newEnd = newEnd.getNext();
                    }
                    rightDir.start = newEnd.getNext();
                    rightDir.count -= minDirectoryCount - dir.count;

                    dir.end = newEnd;
                    dir.count = minDirectoryCount;
                } else {
                    //merge into one
                    rightDir.start = dir.start;
                    rightDir.count += dir.count;
                    directories = (Directory[]) Directory.INSTANCE.removeFromArray(
                            directories, directoryCount, changedDirPos);
                    directoryCount--;
                }
            } else {
                //rebalance with changedDirPos - 1
                Directory leftDir = directories[changedDirPos - 1];
                if (leftDir.count + dir.count >= (minDirectoryCount << 1)) {
                    //move records from leftDir to dir
                    leftDir.count -= minDirectoryCount - dir.count;

                    FSRecord newEnd = leftDir.start;
                    for (int i = 1; i < leftDir.count; i++) {
                        newEnd = newEnd.getNext();
                    }
                    leftDir.end = newEnd;

                    dir.start = newEnd.getNext();
                    dir.count = minDirectoryCount;
                } else {
                    //merge into one
                    dir.start = leftDir.start;
                    dir.count += leftDir.count;

                    directories = (Directory[]) Directory.INSTANCE.removeFromArray(
                            directories, directoryCount, changedDirPos - 1);
                    directoryCount--;
                }
            }
        }
    }


    protected void addRowInner(FSRecord record) {
        int allocatePos = checkNeedSplitIfAdd(record);
        if (allocatePos > 0) {
            DbException.throwInternalError(
                    "alocate pos < 0 while trying to split page. " + toString());
        }

        addRow(record, -allocatePos);
    }

    abstract public int getMemory();

    abstract public void remapChildren();

    abstract public FSRecord splitPage(int splitPoint, PageBase newPage);

    public long getPageId() {
        return pageId;
    }

    public void setPageId(long pageId) {
        if (pageId == this.pageId) {
            return;
        }
        this.pageId = pageId;
        remapChildren();
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
