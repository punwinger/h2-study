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
import org.h2.faststore.Util;
import org.h2.faststore.type.FSRecord;
import org.h2.message.DbException;

//TODO handle overflow data in primary index.
public class FSLeafPage extends PageBase {
    //redo & undo just need to write the change part...

    //entryCount
//    private FSRecord[] records;
//    private int[] offsets;

    private int maxDirectoryCount;
    private int minDirectoryCount;
    private int pageSize;

    private SpaceManager spaceManager;

    private long nextPageId;

    public FSLeafPage(long pageId, FSIndex index, int minDirectoryCount,
                      int maxDirectoryCount, int pageSize) {
        super(pageId, index);
        this.minDirectoryCount = minDirectoryCount;
        this.maxDirectoryCount = maxDirectoryCount;
        this.pageSize = pageSize;
        spaceManager = new SpaceManager(pageSize);
    }

    public static FSLeafPage create(long pageId, FSIndex index) {
        return new FSLeafPage(pageId, index, MIN_DIRECTORY_SIZE,
                MAX_DIRECTORY_SIZE, PAGE_SIZE);
    }




    // return pos. if > 0 then need split at pos. if <= 0 then allocate at pos.
    public int checkNeedSplitIfAdd(FSRecord record) {
        int recordSize = index.getRecordSize(data, record);
        if (recordSize >= MAX_RECORD_SIZE) {
            //record over flow, need overflow page.
            //TODO add overflow page id size?
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

    public void splitPage(int splitPoint, FSLeafPage nextPage) {
        if (splitPoint >= entryCount || splitPoint <= 0) {
            DbException.throwInternalError(
                    "split point " + splitPoint + " invalid. entryCount:" + entryCount);
        }

        spaceManager = new SpaceManager(pageSize);
        FSRecord iter = directories[0].start;
        //read all record
        directories = null;
        directoryCount = 0;
        entryCount = 0;

        //TODO handle overflow record.
        for (int i = 0; i < splitPoint; i++) {
            //addRow will adjust setNext.
            FSRecord add = iter;
            iter = iter.getNext();
            addRowInner(add);
        }

        while (iter != null) {
            FSRecord add = iter;
            iter = iter.getNext();
            nextPage.addRowInner(add);
        }
    }

    public int addRow(FSRecord record, int allocatePos) {
        boolean overflow = false;
        int recordSize = index.getRecordSize(data, record);
        if (recordSize >= MAX_RECORD_SIZE) {
            //record over flow, need overflow page.
            //TODO add overflow page id size?
            recordSize = index.getRecordIndexSize(data, record);
            overflow = true;
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
            entryCount = 1;
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
                int cmp = index.compareKeys(cur, record);
                if (cmp == 0) {
                    checkDuplicate(record, true);
                    cmp = index.compareInnerKey(cur, record);
                    if (cmp == 0) {
                        throw index.getDuplicateKeyException(record.toString());
                    }
                }

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

    //return new max value if max has changed or null if not changed
    //parent can fix index
    public FSRecord removeRow(FSRecord record) {
        int dirPos = 0;
        if (entryCount <= 0) {
            return null;
        }

        FSRecord del = null, newMax = null;
        dirPos = find(record, false);
        if (dirPos >= 0) {
            Directory dir = directories[dirPos];
            del = dir.end;
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

            if (--entryCount > 0 && dirPos == directoryCount - 1) {
                newMax = directories[dirPos - 1].end;
            }
        } else {
            dirPos = -dirPos - 1;
            if (dirPos >= directoryCount) {
                //record not found
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

        if (del != null) {
            //TODO handle over flow
            spaceManager.deallocate(del.getOffset(), index.getRecordSize(data, del));
        }

        //try rebalance directory
        if (directoryCount > 1 && dirPos < directoryCount
                && directories[dirPos].count < minDirectoryCount) {
            Directory dir = directories[dirPos];
            if (dirPos < directoryCount - 1) {
                //rebalance with dirPos + 1
                Directory rightDir = directories[dirPos + 1];
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
                            directories, directoryCount, dirPos);
                    directoryCount--;
                }
            } else {
                //rebalance with dirPos - 1
                Directory leftDir = directories[dirPos - 1];
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
                            directories, directoryCount, dirPos - 1);
                    directoryCount--;
                }

            }
        }

        return newMax;
    }

    @Override
    public int getMemory() {
        return 0;
    }


    @Override
    public boolean isLeaf() {
        return true;
    }

    public long getNextPageId() {
        return nextPageId;
    }

    public void setNextPageId(long nextPageId) {
        this.nextPageId = nextPageId;
    }


    private void addRowInner(FSRecord record) {
        int allocatePos = checkNeedSplitIfAdd(record);
        if (allocatePos > 0) {
            DbException.throwInternalError(
                    "alocate pos < 0 while trying to split page. " + toString());
        }

        addRow(record, -allocatePos);
    }

    @Override
    public String toString() {
        return "entryCount:" + entryCount
                + " directoryCount:" + directoryCount + " directories:"
                + Util.array2String(directories, directoryCount) + " " + spaceManager.toString();
    }
}
