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

import org.h2.faststore.FSTable;
import org.h2.faststore.lock.LockBase;
import org.h2.faststore.type.FSRecord;
import org.h2.index.BaseIndex;

abstract public class FSIndex extends BaseIndex implements LockBase {

    private FSTable table;


    public FSIndex(FSTable table) {
        this.table = table;
    }


    // NOTE: only compare index columns
    public int compareRecord(FSRecord a, FSRecord b) {
        return a.compare(b, columnIds, indexColumns, table.getCompareMode());
    }

}
