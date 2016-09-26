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
import org.h2.result.SearchRow;
import org.h2.store.Data;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.value.CompareMode;
import org.h2.value.Value;

public class FSSimpleLeaf extends FSLeaf {

    private static final int SHORT_INT_SIZE = 2;

    //if redo/undo, need write the whole page to log
    //NOTE: grow from the end of the page
    private FSRecord[] records;
    //private int[] offsets;

    private int pageSize;

    public FSSimpleLeaf(IndexColumn[] indexColumns,
                        int[] columnIds, CompareMode compareMode, int columnCount) {
        this(indexColumns, columnIds, compareMode, columnCount, PAGE_SIZE);
    }

    public FSSimpleLeaf(IndexColumn[] indexColumns,
                        int[] columnIds, CompareMode compareMode, int columnCount, int pageSize) {
        super(indexColumns, columnIds, compareMode, columnCount);
        this.pageSize = pageSize;
    }

    @Override
    public FSRecord findRecord(FSRecord cmpRecord, boolean compareInnerKey) {
        int idx = find(cmpRecord, compareInnerKey);
        return idx >= 0 ? records[idx] : null;
    }

    //if < 0, l is the smallest one bigger than cmpRecord. l is [0, entryCount]
    public int find(FSRecord cmpRecord, boolean compareInnerKey) {
        int l = 0, r = entryCount - 1;
        while (l <= r) {
            int m = (l + r) >>> 1;
            FSRecord mid = records[m];
            int c = mid.compare(cmpRecord, columnIds, indexColumns, compareMode);
            if (c == 0 && compareInnerKey) {
                c = compareInnerKey(mid, cmpRecord);
                if (c == 0) {
                    return m;
                }
            }
            if (c == 0) {
                return m;
            } else if (c > 0) {
                r = m - 1;
            } else {
                l = m + 1;
            }
        }

        return -(l + 1);
    }

    //TODO change data's content
    //link each record?
    @Override
    public int addRow(FSRecord record) {
        int recordSize = getRowSize(record);

        //grow from the end of the page
        //int lastOffest = entryCount == 0 ? pageSize : offsets[entryCount - 1];

        //grow from the end of the page
        int lastOffest = entryCount == 0 ? pageSize : records[entryCount - 1].getOffset();

        if (lastOffest - recordSize < start + SHORT_INT_SIZE) {
            if (entryCount == 0) {
                //don't handle overflow...
                DbException.throwInternalError();
            }

            //need split
            int insertPoint = find(record, true);
            if (insertPoint >= 0) {
                //duplicate insert
                //DbException.throwInternalError();
                return -1;
            }

            insertPoint = -insertPoint - 1;

            if (entryCount < 5) {
                return entryCount / 2;
            } else {
                int third = entryCount / 3;

                return insertPoint <= third ? third :
                        (insertPoint >= third * 2 ? third * 2 : insertPoint);
            }
        }

        int insertPoint = 0;
        if (entryCount > 0) {
            insertPoint = find(record, true);
            if (insertPoint >= 0) {
                //duplicate insert
                //DbException.throwInternalError();
                return -1;
            }
            insertPoint = -insertPoint - 1;
        }

        //int offset = (insertPoint == 0 ? pageSize : offsets[insertPoint - 1]) - recordSize;
        //offsets = insertInArray(offsets, entryCount, insertPoint, offset);
        //now we have entryCount + 1 element in offsets
        //addElement(offsets, insertPoint + 1, entryCount + 1, -recordSize);

        int offset = (insertPoint == 0 ? pageSize
                : records[insertPoint - 1].getOffset()) - recordSize;
        record.setOffset(offset);
        addOffset(insertPoint + 1, entryCount, -recordSize);

        records = insertInArray(records, entryCount, insertPoint, record);
        entryCount++;

        //TODO start should be zero?
//        start += SHORT_INT_SIZE;
        return -1;
    }

    //TODO change data's content
    @Override
    public FSRecord removeRow(FSRecord record) {
        int at = find(record, true);
        if (at < 0) {
            return null;
        }

//        int preOffset = at == 0 ? pageSize : offsets[at - 1];
//        int recordSize = preOffset - offsets[at];
//
//        offsets = removeFromArray(offsets, entryCount, at);
//        addElement(offsets, at, entryCount - 1, recordSize);

        int preOffset = at == 0 ? pageSize : records[at - 1].getOffset();
        int recordSize = preOffset - records[at].getOffset();

        records = removeFromArray(records, entryCount, at);
        entryCount--;

        addOffset(at, entryCount, recordSize);
//        start -= SHORT_INT_SIZE;

        //忽略更新父节点
        return null;
    }

    //add element in array in range [from, to)
    private void addOffset(int from, int to, int x) {
        for (int i = from; i < to; i++) {
            int offset = records[i].getOffset();
            records[i].setOffset(offset + x);
        }
    }

}
