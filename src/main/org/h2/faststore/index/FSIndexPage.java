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

import org.h2.faststore.type.FSRecord;
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


    public FSIndexPage(FSIndex index) {
        super(index);
    }

    @Override
    public boolean tryFastAddRow(FSRecord row) {
        return false;
    }

    @Override
    public long realAddRow(FSRecord row) {
        return 0;
    }
}
