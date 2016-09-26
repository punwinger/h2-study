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
public class FSIndexPage extends PageBase {

    //node
    //array(key, key, key, ...)

    //entryCount = childPageIds.length = keys.length + 1
    // maxKey(childPageIds[i]) <= keys[i] < anyKey(childPageIds[i + 1])
    private FSRecord[] keys;
    protected long[] childPageIds;


    public FSIndexPage(long pageId, FSIndex index) {
        super(pageId, index);
    }

    @Override
    public boolean tryFastAddRow(FSRecord row) {
//        int pos = binarySearch(row);
//
//        if (pos < 0) {
//            pos = -pos - 1;
//            long pageId = childPageIds[pos];
//
//
//        } else {
//            if (add && index.indexType.isUnique()) {
//                if (!index.containsNullAndAllowMultipleNull(compare)) {
//                    throw index.getDuplicateKeyException(compare.toString());
//                }
//            }
//        }

        return false;
    }

    @Override
    public long realAddRow(FSRecord row) {
        return 0;
    }

    @Override
    public FSRecord getMinMaxKey(boolean min) {
        if (isEmptyPage()) {
            throw DbException.get(ErrorCode.CHECK_CONSTRAINT_INVALID, "Node Page "
                    + getPageId() + " is empty");
        }

        return min ? keys[0] : keys[entryCount - 1];
    }

    @Override
    public int binarySearch(FSRecord target, boolean bigger, boolean compareInnerKey) {
        int l = 0, r = keys.length - 1;

        int m = 0;
        while (l <= r) {
            m = (l + r) >>> 1;
            int cmp = index.compareKeys(keys[m], target);
            if (cmp == 0) {
                if (compareInnerKey) {
                    cmp = index.compareInnerKey(keys[m], target);
                    if (cmp == 0) {
                        return m;
                    }
                }
                //return m;
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
}
