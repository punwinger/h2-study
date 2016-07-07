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
 * operationally undo can work concurrent, physically can be faster only single thread
 *
 */
public class FSIndexPage {


}
