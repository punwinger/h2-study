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

import org.h2.result.SearchRow;

public class FSLeafPage extends PageBase {
    @Override
    public boolean tryFastAddRow(SearchRow row) {
        return false;
    }

    @Override
    public long realAddRow(SearchRow row) {
        return 0;
    }

    @Override
    public boolean isLeaf() {
        return true;
    }
}
