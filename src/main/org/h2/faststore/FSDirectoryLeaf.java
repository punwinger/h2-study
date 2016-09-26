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

import org.h2.faststore.type.FSRecord;
import org.h2.message.DbException;
import org.h2.table.IndexColumn;
import org.h2.value.CompareMode;

import java.util.Arrays;

//TODO 是否compact. compact由于一般需要记录整个页的数据，会大大增加redo/undo日志大小
public class FSDirectoryLeaf extends FSLeaf {

    private static final int MAX_DIRECTORY_SIZE = 8;

    //redo & undo just need to write the change part...
    // records[i - 1] < directories[i] <= records[i]
    // directories[0] is the smallest
    //TODO need to write to disk??
    private Directory[] directories;
    private int directoryCount;

    //entryCount
//    private FSRecord[] records;
//    private int[] offsets;

    private int directorySize;
    private SpaceManager spaceManager;

    private static class SpacePos {
        public int left;  //include
        public int right;  //include

        public SpacePos(int l, int r) {
            left = l;
            right = r;
        }

        public int getSize() {
            return right - left + 1;
        }

        @Override
        public String toString() {
            return "Pos l:" + left + " r:" + right;
        }
    }

    private static class SpaceManager {
        private SpacePos[] poses;
        private int count;

        public SpaceManager(int left, int right) {
            poses = new SpacePos[COPY_THRESHOLD + 1];
            poses[0] = new SpacePos(left, right);
            count = 1;
        }

        public SpaceManager(int size) {
            this(0, size - 1);
        }

        // O(N)
        //TODO better algorithm in O(logN)?
        public int tryAllocate(int size) {
            for (int i = 0; i < count; ++i) {
                int sz = poses[i].getSize();
                if (sz >= size) {
                    return i;
                }
            }
            return -1;
        }

        public int allocate(int pos, int size) {
            if (pos < 0 || pos >= count) {
                return -1;
            }

            int offset = -1;
            int sz = poses[pos].getSize();
            if (sz == size) {
                offset = poses[pos].left;
                poses = removeFromArray(poses, count, pos);
                count--;
            } else if (sz > size) {
                offset = poses[pos].left;
                poses[pos].left += size;
            }

            return offset;
        }

        // O(logN)
        public void deallocate(int offset, int size) {
            int l = 0, r = count - 1;
            while (l <= r) {
                int m = (l + r) >>> 1;
                if (poses[m].left == offset) {
                    DbException.throwInternalError();
                } else if (poses[m].left > offset) {
                    r = m - 1;
                } else {
                    l = m + 1;
                }
            }

            //l is the smallest one bigger than offset
            boolean hasMerge = false;
            //merge into right space
            if (l < count && offset + size == poses[l].left) {
                poses[l].left = offset;
                hasMerge = true;
            }

            //merge into left space
            if (l > 0 && poses[l - 1].right + 1 == offset) {
                if (hasMerge) {
                    poses[l - 1].right = poses[l].right;
                    poses = removeFromArray(poses, count, l);
                    count--;
                    return;
                }

                poses[l - 1].right += size;
                hasMerge = true;
            }

            if (!hasMerge) {
                poses = insertInArray(poses, count, l, new SpacePos(offset, offset + size - 1));
                count++;
            }
        }

        @Override
        public String toString() {
            return "[Space Detail: count:" + count + " " + Util.array2String(poses, count) + "]";
        }
    }

    private static class Directory {
        public int count;
        public FSRecord start;
        public FSRecord end;

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
    }

    public FSDirectoryLeaf(IndexColumn[] indexColumns,
                           int[] columnIds, CompareMode compareMode, int columnCount) {
        this(indexColumns, columnIds, compareMode, columnCount, MAX_DIRECTORY_SIZE, PAGE_SIZE);
    }

    //for test
    public FSDirectoryLeaf(IndexColumn[] indexColumns, int[] columnIds,
                           CompareMode compareMode, int columnCount, int directorySize, int pageSize) {
        super(indexColumns, columnIds, compareMode, columnCount);
        this.directorySize = directorySize;
        spaceManager = new SpaceManager(pageSize);
    }

    public int directoryCount() {
        return directoryCount;
    }

    @Override
    public FSRecord findRecord(FSRecord cmpRecord, boolean compareInnerKey) {
        int idx = findDirectory(cmpRecord, compareInnerKey);
        if (idx >= 0) {
            return directories[idx].start;
        }

        idx = -idx - 2;
        if (idx < 0) {
            return null;
        }
        FSRecord record = directories[idx].start;
        while (record.getNext() != null) {
            record = record.getNext();
            int cmp = record.compare(cmpRecord, columnIds, indexColumns, compareMode);
            if (cmp == 0) {
                return record;
            } else if (cmp > 0) {
                break;
            }
        }

        return null;
    }

    //if < 0, r is biggest one smaller than cmpRecord. r is [-1, entryCount - 1]
    private int findDirectory(FSRecord cmpRecord, boolean compareInnerKey) {
        int l = 0, r = directoryCount - 1;
        while (l <= r) {
            int m = (l + r) >>> 1;
            FSRecord record = directories[m].start;
            int c = record.compare(cmpRecord, columnIds, indexColumns, compareMode);
            if (c == 0 && compareInnerKey) {
                c = compareInnerKey(record, cmpRecord);
            }

            if (c == 0) {
                return m;
            } else if (c > 0) {
                r = m - 1;
            } else {
                l = m + 1;
            }
        }
        return -(r + 2);
    }

    //unique insert. no duplicate allow
    @Override
    public int addRow(FSRecord record) {
        //check overflow
        int recordSize = getRowSize(record);
        int pos = spaceManager.tryAllocate(recordSize);
        if (pos < 0) {
            if (entryCount == 0) {
                //don't handle overflow...
                DbException.throwInternalError("add record:" + record + " state:" + toString());
            }

            int idx = findDirectory(record, true);
            if (idx >= 0) {
                //duplicate insert
                //DbException.throwInternalError();
                return -1;
            }

            idx = -idx - 2;

            if (directoryCount < 5) {
                return directoryCount / 2;
            } else {
                int third = directoryCount / 3;

                return idx < third ? third :
                        (idx >= third * 2 ? third * 2 : idx);
            }
        }

        if (entryCount == 0) {
            Directory dir = new Directory(record, record, 1);
            directories = insertInArray(directories, directoryCount, 0, dir);
            directoryCount++;
            entryCount = 1;
            int offset = spaceManager.allocate(pos, recordSize);
            if (offset < 0) {
                DbException.throwInternalError("allocate error. " + toString());
            }
            record.setOffset(offset);
            return -1;
        }

        //find insertPoint of directories
        int insertPoint, insertPosInDir = 0;
        insertPoint = findDirectory(record, true);
        if (insertPoint >= 0) {
            //duplicate insert
            //DbException.throwInternalError("Duplicate record");
            return -1;
        }
        insertPoint = -insertPoint - 2;
        Directory dir = null;
        if (insertPoint < 0) {
            //record < directories[0].start
            insertPoint = insertPosInDir = 0;
            dir = directories[insertPoint];
            record.setNext(dir.start);
            dir.start = record;
        } else {
            //insert into directories
            dir = directories[insertPoint];
            FSRecord prev = dir.start;

            boolean hasInsert = false;
            for (insertPosInDir = 1; insertPosInDir <= dir.count; ++insertPosInDir) {
                FSRecord cur = prev.getNext();
                if (cur == null) {
                    prev.setNext(record);
                    hasInsert = true;
                    break;
                }
                int cmp = compareRecord(cur, record);
                if (cmp == 0) {
                    //DbException.throwInternalError("Duplicate record");
                    return -1;
                }
                if (cmp > 0) {
                    record.setNext(cur);
                    prev.setNext(record);
                    hasInsert = true;
                    break;
                }
                prev = cur;
            }

            if (!hasInsert) {
                if (insertPoint == directoryCount - 1) {
                    dir.end.setNext(record);
                    record.setNext(null);
                } else {
                    DbException.throwInternalError("Directory cannot find pos to insert. insert:"
                                    + record + " dir:" + dir + " detail:" + toString());
                }
            }

            if (insertPosInDir == dir.count) {
                dir.end = record;
            }
        }

        if (++dir.count > directorySize) {
            //split directories
            int third = dir.count / 3;
            int splitPoint = insertPosInDir <= third ? third :
                    (insertPosInDir >= third * 2 ? third * 2 : insertPosInDir);

            FSRecord newEnd = dir.start;
            for (int i = 1; i < splitPoint; i++) {
                newEnd = newEnd.getNext();
            }

            Directory newDir = new Directory(newEnd.getNext(), dir.end, dir.count - splitPoint);
            dir.end = newEnd;
            dir.count = splitPoint;
            directories = insertInArray(directories, directoryCount, insertPoint + 1, newDir);
            directoryCount++;
        }

        int offset = spaceManager.allocate(pos, recordSize);
        if (offset < 0) {
            DbException.throwInternalError("allocate error. " + toString());
        }
        record.setOffset(offset);
        entryCount++;
        return -1;
    }

    @Override
    public FSRecord removeRow(FSRecord record) {
        int dirPos = 0;
        if (entryCount <= 0) {
           return null;
        }

        FSRecord del = null;
        dirPos = findDirectory(record, true);
        if (dirPos >= 0) {
            Directory dir = directories[dirPos];
            del = dir.start;
            if (dir.count == 1) {
                //remove the directory
                directories = removeFromArray(directories, directoryCount, dirPos);
                directoryCount--;
            } else {
                dir.start = del.getNext();
                dir.count--;
            }

            if (dirPos > 0) {
                //adjust the prev
                FSRecord lastEnd = directories[dirPos - 1].end;
                FSRecord newNext = dirPos < directoryCount ? directories[dirPos].start : null;
                lastEnd.setNext(newNext);
            }

            entryCount--;
        } else {
            dirPos = -dirPos - 2;
            if (dirPos < 0) {
                //record not found
                return null;
            }

            Directory dir = directories[dirPos];
            FSRecord prev = dir.start;
            for (int i = 1; i < dir.count; i++) {
                FSRecord cur = prev.getNext();
                int cmp = compareRecord(cur, record);
                if (cmp > 0) {
                    //not found
                    return null;
                } else if (cmp == 0) {
                    prev.setNext(cur.getNext());
                    if (dir.end == cur) {
                        dir.end = prev;
                    }
                    del = cur;
                    dir.count--;
                    entryCount--;
                    break;
                }

                prev = cur;
            }
        }

        if (del != null) {
            spaceManager.deallocate(del.getOffset(), getRowSize(del));
        }

        return null;
    }

    protected static SpacePos[] removeFromArray(SpacePos[] old, int oldSize, int pos) {
        SpacePos[] result;
        if (old.length <= oldSize + DELETE_THRESHOLD) {
            result = old;
        } else {
            result = new SpacePos[oldSize + COPY_THRESHOLD - 1];
            if (pos > 0) {
                System.arraycopy(old, 0, result, 0, pos);
            }
        }

        if (oldSize - pos - 1 > 0) {
            System.arraycopy(old, pos + 1, result, pos, oldSize - pos - 1);
        }

        return result;
    }

    protected static Directory[] removeFromArray(Directory[] old, int oldSize, int pos) {
        Directory[] result;
        if (old.length <= oldSize + DELETE_THRESHOLD) {
            result = old;
        } else {
            result = new Directory[oldSize + COPY_THRESHOLD - 1];
            if (pos > 0) {
                System.arraycopy(old, 0, result, 0, pos);
            }
        }

        if (oldSize - pos - 1 > 0) {
            System.arraycopy(old, pos + 1, result, pos, oldSize - pos - 1);
        }

        return result;
    }

    protected static SpacePos[] insertInArray(SpacePos[] old, int oldSize, int pos, SpacePos x) {
        SpacePos[] result;
        if (old != null && old.length > oldSize) {
            result = old;
        } else {
            result = new SpacePos[oldSize + 1 + COPY_THRESHOLD];
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

    protected static Directory[] insertInArray(Directory[] old, int oldSize, int pos, Directory x) {
        Directory[] result;
        if (old != null && old.length > oldSize) {
            result = old;
        } else {
            result = new Directory[oldSize + 1 + COPY_THRESHOLD];
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

    @Override
    public String toString() {
        return "entryCount:" + entryCount
                + " directoryCount:" + directoryCount + " directories:"
                + Util.array2String(directories, directoryCount) + " " + spaceManager.toString();
    }

    //test
    //FSRecord in directories
    public boolean checkLink() {
        int i = 0;
        FSRecord iter = directories[i].start;
        while (iter != null && i < directoryCount) {
            Directory dir = directories[i];
            if (iter != dir.start) {
                return false;
            }

            for (int j = 0; j < dir.count - 1; j++) {
                FSRecord next = iter.getNext();
                if (compareRecord(next, iter) <= 0) {
                    return false;
                }
                iter = next;
            }

            if (iter != dir.end) {
                return false;
            }

            FSRecord next = iter.getNext();
            if (next != null && compareRecord(next, iter) <=0 ) {
                return false;
            }

            iter = next;
            i++;
        }

        return true;
    }

    //test
    //check offset ... offset+len-1 in/not in space
    public boolean checkSpace(int offset, int len, boolean inUse) {
        int left = offset;
        int right = offset + len - 1;

        if (left < 0 || left > right) {
            return false;
        }

        if (inUse) {
            //not in space
            for (int i = 0; i < spaceManager.count; i++) {
                if (left > spaceManager.poses[i].right) {
                    if (i < spaceManager.count - 1 && right < spaceManager.poses[i + 1].left) {
                        return true;
                    } else if (i == spaceManager.count - 1) {
                        return true;
                    }
                }

                if (i == 0 && right < spaceManager.poses[i].left) {
                    return true;
                }
            }
            return false;
        } else {
            //in space
            for (int i = 0; i < spaceManager.count; i++) {
                if (left >= spaceManager.poses[i].left && right <= spaceManager.poses[i].right) {
                    return true;
                }
            }
            return false;
        }
    }
}
