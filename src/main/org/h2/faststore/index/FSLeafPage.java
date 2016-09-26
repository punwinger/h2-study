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
import org.h2.faststore.type.FSRecord;
import org.h2.message.DbException;
import org.h2.result.SearchRow;

public class FSLeafPage extends PageBase {

    //leaf
    //array(record -> record -> record -> ...) + directory
    //
    // directory flush to disk?
    // how to adjust?
    // rebuild dynamic?
    private FSRecord[] directory;

    // records[i] <= directory[i] < records[i ＋ 1]s
    // link each other
    private FSRecord[] records;

    private long nextPageId;

    public FSLeafPage(long pageId, FSIndex index) {
        super(pageId, index);
    }

    public static FSLeafPage create(long pageId, FSIndex index) {
        return new FSLeafPage(pageId, index);
    }

    @Override
    public boolean tryFastAddRow(FSRecord row) {
        return false;
    }

    @Override
    public long realAddRow(FSRecord row) {
        return 0;
    }

    @Override
    public FSRecord getMinMaxKey(boolean min) {
        if (isEmptyPage()) {
            throw DbException.get(ErrorCode.GENERAL_ERROR_1, "Leaf Page "
                    + getPageId() + " is empty");
        }

        return null;
    }

    @Override
    public int binarySearch(FSRecord target, boolean bigger, boolean compareInnerKey) {
        int l = 0, r = directory.length - 1;
        while (l <= r) {
            int m = (l + r) >>> 1;
            int cmp = index.compareKeys(directory[m], target);
            if (cmp == 0) {
                if (compareInnerKey) {
                    cmp = index.compareInnerKey(directory[m], target);
                    if (cmp == 0) {
                        return m;
                    }
                }
            }

            if (cmp > 0 || (!bigger && cmp == 0)) {
                r = m - 1;
            } else {
                l = m + 1;
            }
        }


        return -(l + 1);
    }

    @Override
    public int getMemory() {
        return 0;
    }

    public FSRecord getRecords(int idx) {
        if (idx < 0 || idx >= records.length) {
            throw DbException.get(ErrorCode.GENERAL_ERROR_1,
                    "index out of bound, from:" + idx + " max:" + records.length);
        }

        return records[idx];
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
}
