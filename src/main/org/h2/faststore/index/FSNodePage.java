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

import org.h2.engine.SysProperties;
import org.h2.faststore.type.FSRecord;
import org.h2.message.DbException;

/**
 * nonleaf page & leaf page?
 *
 * nonleaf:
 * no MVCC info
 * only key store
 *
 * leaf:
 * MVCC
 * key + value
 *
 * redo & logical undo problem? physically redo or operationally undo(mvcc)
 * operationally undo can work concurrent, physically can be faster with single thread
 *
 */
public class FSNodePage extends PageBase {
    //entryCount == record num (maxRecord exclude)

    //the last record in the record link but not in directory.
    private FSRecord maxRecord = new FSRecord(0);


    public FSNodePage(long pageId, FSIndex index,int minDirectoryCount,
                      int maxDirectoryCount, int pageSize) {
        super(pageId, index, minDirectoryCount, maxDirectoryCount, pageSize);
    }

    public static FSNodePage create(long pageId, FSIndex index) {
        return new FSNodePage(pageId, index, MIN_DIRECTORY_NUM,
                MAX_DIRECTORY_NUM, PAGE_SIZE);
    }

    public void initPage(long page1, FSRecord pivot, long page2) {
        entryCount = 0;
        directoryCount = 0;
        directories = null;

        maxRecord.setChildPageId(page2);
        int pos = checkNeedSplitIfAdd(pivot);
        if (pos > 0) {
            DbException.throwInternalError("pos " + pos + " invalid while initPage");
        }
        pivot.setChildPageId(page1);
        addRow(pivot, -pos);
    }

    //search: return the smallest one >= cmpRecord. compareInnerKey
    //add:   duplicate check & smallest one > record in directory & compareInnerKey
    //del:   compareInnerKey
    public long findPage(FSRecord cmpRecord, boolean add) {
        FSRecord r = findRecord(cmpRecord, false);
        if (r == null) {
            r = maxRecord;
        }
        return r.getChildPageId();
    }


    public void addChild(long oldPageId, FSRecord pivot, long newPageId, FSRecord oldKey) {
        FSRecord myOldKey = findRecord(oldKey, false);
        if (SysProperties.CHECK) {
            if (index.compareKeys(oldKey, myOldKey) != 0
                    || index.compareInnerKey(oldKey, myOldKey) != 0) {
                DbException.throwInternalError(
                        "addChild find different key. oldKey:" + oldKey + " myOldKey:" + myOldKey);
            }
        }

        myOldKey.setChildPageId(newPageId);
        int pos = checkNeedSplitIfAdd(pivot);
        if (pos > 0) {
            DbException.throwInternalError("addChild need split.");
        }

        pivot.setChildPageId(oldPageId);
        addRow(pivot, -pos);
    }

    @Override
    public void remapChildren() {
        //TODO need latch? only struct-modify-operation need traversal back...
        if (directoryCount > 0) {
            FSRecord iter = directories[0].start;
            while (iter != null) {
                PageBase page = index.getStore().getPage(iter.getChildPageId());
                page.setParentPageId(getPageId());
                iter = iter.getNext();
            }
        }

        long pageId = maxRecord.getChildPageId();
        if (pageId != INVALID_PAGE_ID) {
            PageBase page = index.getStore().getPage(pageId);
            page.setParentPageId(getPageId());
        }
    }

    @Override
    public FSRecord splitPage(int splitPoint, PageBase newPage) {
        if (splitPoint >= entryCount || splitPoint <= 0) {
            DbException.throwInternalError(
                    "split point " + splitPoint + " invalid. entryCount:" + entryCount);
        }

        FSRecord iter = directories[0].start;
        //todo read all record
        reset();

        FSRecord pivot = null, add = null;
        FSNodePage newNode = (FSNodePage)newPage;
        long lastChild = maxRecord.getChildPageId();
        //TODO handle overflow record.
        for (int i = 0; i < splitPoint - 1; i++) {
            //addRow will adjust setNext.
            add = iter;
            iter = iter.getNext();
            addRowInner(add);
        }
        pivot = iter;
        maxRecord.setChildPageId(pivot.getChildPageId());
        iter = iter.getNext();

        while (iter != null) {
            newNode.addRowInner(iter);
            iter = iter.getNext();
        }
        newNode.maxRecord.setChildPageId(lastChild);
        newNode.remapChildren();

        return pivot;
    }

    @Override
    public void free() {
        super.free();
        maxRecord = null;
    }

    @Override
    public FSRecord getMinMaxKey(boolean min) {
        if (!isEmptyPage() && entryCount == 0) {
            return null;
        } else {
            return super.getMinMaxKey(min);
        }
    }

    @Override
    public FSRecord removeRow(FSRecord record) {
        if (entryCount == 0) {
            //only maxRecord
            checkIsMaxRecordChildPageId(record.getChildPageId());
            return record;
        }

        FSRecord newLast = super.removeRow(record);
        //return:
        // 1.null if last record in directory not change or not find
        // 2.record if delete last record in directory
        // 3.newLast if last record in directory change
        if (newLast == null) {
            //last record in directory
            FSRecord dirMaxRecord = directories[directoryCount - 1].end;
            if (maxRecord.getChildPageId() == record.getChildPageId()) {
                //remove last row in directory
                removeDirectoryRecord(directoryCount - 1);
                maxRecord.setChildPageId(dirMaxRecord.getChildPageId());
                return dirMaxRecord;
            } else {
                if (SysProperties.CHECK &&
                        index.compareAllKeys(dirMaxRecord, record, false) < 0) {
                    //delete a record > max record in directory
                    //but not in page of maxRecord.getChildPageId()
                    DbException.throwInternalError();
                }
            }
        }

        return null;
    }


    public long getMaxRecordChildPageId() {
        return maxRecord.getChildPageId();
    }

    //return:
    // < 0 if can not allocate newSize
    // > 0 for allocate pos
    public int allocateNewAndDeallocateOld(FSRecord oldRecord, FSRecord newRecord) {
        int oldRecordSize = index.getRecordIndexSize(data, oldRecord);
        int newRecordSize = index.getRecordIndexSize(data, newRecord);

        int pos = spaceManager.allocateNewAndDeallocateOld(newRecordSize,
                oldRecord.getOffset(), oldRecordSize);

        if (pos > 0) {
            int offset = spaceManager.allocate(pos, newRecordSize);
            newRecord.setOffset(offset);
        }

        return pos;
    }

    public void fixEntryInChild(FSRecord oldMax, FSRecord newMax) {
        //find the record == oldMax in directory or not found (it's in maxRecord)
        int idx = find(oldMax, false);
        FSRecord prevRecord = null, myOldMax = null;

        if (idx >= 0) {
            Directory dir = directories[idx];
            myOldMax = dir.end;
            if (dir.count == 1) {
                dir.start = dir.end = newMax;

                if (idx > 0) {
                    prevRecord = directories[idx - 1].end;
                }
            } else {
                prevRecord = dir.start;
                for (int i = 1; i < dir.count - 1; i++) {
                    prevRecord = prevRecord.getNext();
                }

                dir.end = newMax;
            }

            if (prevRecord != null) {
                prevRecord.setNext(newMax);
            }

            newMax.setNext(myOldMax.getNext());
        } else {
            idx = -idx - 1;
            if (idx >= directoryCount) {
                //not in childPage of maxRecord
                DbException.throwInternalError();
            }

            //dir.count > 1
            Directory dir = directories[idx];
            prevRecord = idx > 0 ? directories[idx - 1].end : null;
            myOldMax = dir.start;
            for (int i = 0; i < dir.count - 1; i++) {
                int cmp = index.compareAllKeys(myOldMax, oldMax, false);
                if (cmp == 0) {
                    if (prevRecord != null) {
                        prevRecord.setNext(newMax);
                    }
                    if (dir.start == myOldMax) {
                        dir.start = newMax;
                    }
                    newMax.setNext(myOldMax.getNext());
                    break;
                }

                if (cmp > 0) {
                    //not found myOldMax == oldMax? it's impossible
                    DbException.throwInternalError();
                }

                prevRecord = myOldMax;
                myOldMax = myOldMax.getNext();
            }
        }
    }

    private void checkIsMaxRecordChildPageId(long childPageId) {
        if (maxRecord.getChildPageId() != childPageId) {
            DbException.throwInternalError(
                    "node page remove different child page. "
                            + maxRecord.getChildPageId() + " : " + childPageId);
        }
    }

    @Override
    public boolean isEmptyPage() {
        return super.isEmptyPage() && maxRecord != null
                && maxRecord.getChildPageId() != INVALID_PAGE_ID;
    }

    @Override
    public int getMemory() {
        return 0;
    }
}
