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

import org.h2.faststore.index.PageBase;

public class FastStore {

    private CacheLongKeyARC<PageBase> cache;

    public FastStore() {

        long maxMemory = 16 * 1024 * 1024;
        int averMemory = 4 * 1024;
        int segmentCount = 16;
        cache = new CacheLongKeyARC<PageBase>(maxMemory, averMemory, segmentCount);
    }


    public PageBase getPage(long pageId) {
        return null;
    }

    public int getPageSplitSize() {
        return 0;
    }

    public void fixPage(long pageId) {
        //TODO CacheLongKeyARC not support fix ...
    }

    public long allocatePage() {
        return 0;
    }
}
