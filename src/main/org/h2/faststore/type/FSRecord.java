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

package org.h2.faststore.type;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.value.CompareMode;
import org.h2.value.Value;

import java.util.Arrays;

public class FSRecord implements SearchRow {

    private int owned;

    private FSRecord next;

    //store whole row, null if not exist
    private Value[] data;

    private long innerKey;

    private int offset = -1;

    public FSRecord(int length) {
        data = new Value[length];
    }

    public int compare(FSRecord other, int[] columnId,
                           IndexColumn[] indexColumns, CompareMode compareMode) {
        if (this == other) {
            return 0;
        }
        for (int i = 0, len = columnId.length; i < len; i++) {
            int index = columnId[i];
            Value b = other.data[index];
            if (b == null) {
                // can't compare further
                return 0;
            }

            //compare values
//            int c = compareValues(data[i], v, indexColumns[i].sortType, compareMode);
//            if (c != 0) {
//                return c;
//            }

            Value a = data[index];
            if (a == null) {
                return SortOrder.compareNull(true, indexColumns[i].sortType);
            }

            // type must be converted before compare
            if (a.getType() != b.getType()) {
                throw DbException.get(ErrorCode.GENERAL_ERROR_1, "type is " +
                        "not equal when compare!");
            }

            int comp = a.compareTypeSave(b, compareMode);
            if ((indexColumns[i].sortType & SortOrder.DESCENDING) != 0) {
                comp = -comp;
            }

            if (comp != 0) {
                return comp;
            }
        }
        return 0;

    }

//    private int compareValues(Value a, Value b, int sortType, CompareMode compareMode) {
//        if (a == b) {
//            return 0;
//        }
//        boolean aNull = a == null, bNull = b == null;
//        if (aNull || bNull) {
//            return SortOrder.compareNull(aNull, sortType);
//        }
//        int comp = table.compareTypeSave(a, b);
//        if ((sortType & SortOrder.DESCENDING) != 0) {
//            comp = -comp;
//        }
//        return comp;
//    }


    @Override
    public int getColumnCount() {
        return 0;
    }

    @Override
    public Value getValue(int index) {
        return data[index];
    }

    @Override
    public void setValue(int index, Value v) {
        data[index] = v;
    }

    @Override
    public void setKeyAndVersion(SearchRow old) {

    }

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public void setKey(long key) {
        this.innerKey = key;
    }

    @Override
    public long getKey() {
        return innerKey;
    }

    @Override
    public int getMemory() {
        return 0;
    }

    public FSRecord getNext() {
        return next;
    }

    public void setNext(FSRecord next) {
        this.next = next;
    }

    public void setOffset (int offset) {
        this.offset = offset;
    }

    public int getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        return "FSRecord key:" + innerKey + " offset:" + offset
                + " data:" + Arrays.toString(data);
    }
}
