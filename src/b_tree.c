#include "b_tree.h"
#include "buffer_pool.h"

#include <stdio.h>

typedef struct {
    RID node_key;
    off_t node_ptr;
} new_node;

new_node allocate_new_node(BufferPool *pool, off_t *node_addr, int is_leaf) {
    new_node allocated_node;
    BNode *new_node;
    BCtrlBlock *bctrl = (BCtrlBlock*)get_page(pool, 0);
    
    if (bctrl->free_node_head != -1) {
        *node_addr = bctrl->free_node_head;
        new_node = (BNode*)get_page(pool, *node_addr);
        bctrl->free_node_head = new_node->next;
    } else {
        *node_addr = pool->file.length;
        new_node = (BNode*)get_page(pool, *node_addr);
    }

    new_node->n = DEGREE + 1;
    new_node->leaf = is_leaf;
    new_node->next = -1;
    
    release(pool, 0);

    allocated_node.node_ptr = *node_addr;
    allocated_node.node_key = new_node->row_ptr[0]; 
    
    return allocated_node;
}

void insert_into_leaf_node(BNode *node, RID rid, b_tree_row_row_cmp_t cmp) {
    int ins_num = 0;
    while (ins_num < node->n && (*cmp)(rid, node->row_ptr[ins_num]) >= 0) {
        ins_num++;
    }

    for (size_t i = node->n; i > ins_num; i--) {
        node->row_ptr[i] = node->row_ptr[i - 1];
    }
    node->row_ptr[ins_num] = rid;
    node->n++;
}
void insert_into_nonleaf_node(BNode *node, new_node newchild, b_tree_row_row_cmp_t cmp) {
    int ins_pos = 0;
    while (ins_pos < node->n && (*cmp)(newchild.node_key, node->row_ptr[ins_pos]) >= 0) {
        ins_pos++;
    }

    for (size_t i = node->n; i > ins_pos; i--) {
        node->row_ptr[i] = node->row_ptr[i - 1];
        node->child[i+1] = node->child[i];
    }

    node->row_ptr[ins_pos] = newchild.node_key;
    node->child[ins_pos + 1] = newchild.node_ptr;
    node->n++;
}

new_node insert_to_Btree(BufferPool *pool, off_t addr, RID rid, b_tree_row_row_cmp_t cmp, off_t root_node_addr, b_tree_insert_nonleaf_handler_t insert_handler) {
    BNode *node = (BNode*)get_page(pool, addr);
    if (node->leaf) {
        if (node->n < 2 * DEGREE) {
            insert_into_leaf_node(node, rid, cmp);
            release(pool, addr);
            new_node nullentry;
            get_rid_block_addr(nullentry.node_key) = -1;
            get_rid_idx(nullentry.node_key) = -1;
            nullentry.node_ptr = -1;
            return nullentry;
        }
        else {
            if (addr == root_node_addr) {
                off_t new_leaf_node_addr;
                BNode *new_leaf_node;
                BCtrlBlock *bctrl = (BCtrlBlock*)get_page(pool, 0);
                if (bctrl->free_node_head != -1) {
                    new_leaf_node_addr = bctrl->free_node_head;
                    new_leaf_node = (BNode*)get_page(pool, new_leaf_node_addr);
                    bctrl->free_node_head = new_leaf_node->next;
                }
                else {
                    new_leaf_node_addr = pool->file.length;
                    new_leaf_node = (BNode*)get_page(pool, pool->file.length);
                }
                off_t new_root_node_addr;
                BNode *new_root_node;
                if (bctrl->free_node_head != -1) {
                    new_root_node_addr = bctrl->free_node_head;
                    new_root_node = (BNode*)get_page(pool, new_root_node_addr);
                    bctrl->free_node_head = new_root_node->next;
                }
                else {
                    new_root_node_addr = pool->file.length;
                    new_root_node = (BNode*)get_page(pool, pool->file.length);
                }
                release(pool, 0);
                new_leaf_node->n = DEGREE + 1;
                new_leaf_node->leaf = 1;
                new_leaf_node->next = -1;
                int child_pos = 0;
                while (child_pos != 2 * DEGREE && (*cmp)(rid, node->row_ptr[child_pos]) >= 0) {
                    child_pos++;
                }

                if (child_pos < DEGREE) {
                    memcpy(new_leaf_node->row_ptr, &node->row_ptr[DEGREE - 1], (DEGREE + 1) * sizeof(*node->row_ptr));
                    for (int i = DEGREE - 1; i > child_pos; i--) {
                        node->row_ptr[i] = node->row_ptr[i - 1];
                    }
                    node->row_ptr[child_pos] = rid;
                }
                else {
                    memcpy(new_leaf_node->row_ptr, node->row_ptr + DEGREE, (child_pos - DEGREE) * sizeof(int));
                    new_leaf_node->row_ptr[child_pos - DEGREE] = rid;
                    memcpy(new_leaf_node->row_ptr + (child_pos - DEGREE + 1), node->row_ptr + DEGREE + child_pos - DEGREE, (DEGREE - (child_pos - DEGREE)) * sizeof(int));
                }
                node->n = DEGREE;
                new_root_node->leaf = 0;
                new_root_node->n = 1;
                new_root_node->child[0] = addr;
                new_root_node->child[1] = new_leaf_node_addr;
                new_root_node->next = -1;
                new_root_node->row_ptr[0] = (*insert_handler)(new_leaf_node->row_ptr[0]);
                release(pool, addr);
                release(pool, new_leaf_node_addr);
                new_node newchild;
                newchild.node_key = new_root_node->row_ptr[0];
                newchild.node_ptr = new_leaf_node_addr;
                release(pool, new_root_node_addr);
                bctrl = (BCtrlBlock*)get_page(pool, 0);
                bctrl->root_node = new_root_node_addr;
                release(pool, 0);
                return newchild;
            }
            else {
                off_t new_leaf_node_addr;
                BNode *new_leaf_node;
                BCtrlBlock *bctrl = (BCtrlBlock*)get_page(pool, 0);
                if (bctrl->free_node_head != -1) {
                    new_leaf_node_addr = bctrl->free_node_head;
                    new_leaf_node = (BNode*)get_page(pool, new_leaf_node_addr);
                    bctrl->free_node_head = new_leaf_node->next;
                }
                else {
                    new_leaf_node_addr = pool->file.length;
                    new_leaf_node = (BNode*)get_page(pool, pool->file.length);
                }
                release(pool, 0);
                new_leaf_node->n = DEGREE + 1;
                new_leaf_node->leaf = 1;
                new_leaf_node->next = -1;
                new_node newchild;
                int child_pos = 0;
                while (child_pos != 2 * DEGREE && (*cmp)(rid, node->row_ptr[child_pos]) >= 0) {
                    child_pos++;
                }

                if (child_pos < DEGREE) {
                    newchild.node_key = (*insert_handler)(node->row_ptr[DEGREE - 1]);
                    newchild.node_ptr = new_leaf_node_addr;
                    memcpy(new_leaf_node->row_ptr, &node->row_ptr[DEGREE - 1], (DEGREE + 1) * sizeof(*node->row_ptr));
                    for (int i = DEGREE - 1; i > child_pos; i--) {
                        node->row_ptr[i] = node->row_ptr[i - 1];
                    }
                    node->row_ptr[child_pos] = rid;
                }
                else {
                    for (int i = 0; i != child_pos - DEGREE; i++) {
                        new_leaf_node->row_ptr[i] = node->row_ptr[DEGREE + i];
                    }
                    new_leaf_node->row_ptr[child_pos - DEGREE] = rid;
                    for (int i = child_pos - DEGREE + 1; i <= DEGREE; i++) {
                        new_leaf_node->row_ptr[i] = node->row_ptr[i + DEGREE - 1];
                    }
                    newchild.node_key = (*insert_handler)(new_leaf_node->row_ptr[0]);
                    newchild.node_ptr = new_leaf_node_addr;
                }
                node->n = DEGREE;
                release(pool, addr);
                release(pool, new_leaf_node_addr);
                return newchild;
            }
        }
    }
    else {
        off_t child_node_addr;
        int child_node_pos = 0;
        while (child_node_pos != node->n && (*cmp)(rid, node->row_ptr[child_node_pos]) >= 0) {
            child_node_pos++;
        }

        child_node_addr = node->child[child_node_pos];
        new_node newchild;
        release(pool, addr);
        newchild = insert_to_Btree(pool, child_node_addr, rid, cmp, root_node_addr, insert_handler);
        node = (BNode*)get_page(pool, addr);
        if (newchild.node_ptr == -1) {
            release(pool, addr);
            return newchild;
        }
        else {
            if (node->n < 2 * DEGREE) {
                insert_into_nonleaf_node(node, newchild, cmp);
                release(pool, addr);
                new_node nullentry;
                get_rid_block_addr(nullentry.node_key) = -1;
                get_rid_idx(nullentry.node_key) = -1;
                nullentry.node_ptr = -1;
                return nullentry;
            }
            else {
                if (addr == root_node_addr) {
                    off_t new_nonleaf_node_addr;
                    BNode *new_nonleaf_node;
                    BCtrlBlock *bctrl = (BCtrlBlock*)get_page(pool, 0);
                    if (bctrl->free_node_head != -1) {
                        new_nonleaf_node_addr = bctrl->free_node_head;
                        new_nonleaf_node = (BNode*)get_page(pool, new_nonleaf_node_addr);
                        bctrl->free_node_head = new_nonleaf_node->next;
                    }
                    else {
                        new_nonleaf_node_addr = pool->file.length;
                        new_nonleaf_node = (BNode*)get_page(pool, pool->file.length);
                    }
                    off_t new_root_node_addr;
                    BNode *new_root_node;
                    if (bctrl->free_node_head != -1) {
                        new_root_node_addr = bctrl->free_node_head;
                        new_root_node = (BNode*)get_page(pool, new_root_node_addr);
                        bctrl->free_node_head = new_root_node->next;
                    }
                    else {
                        new_root_node_addr = pool->file.length;
                        new_root_node = (BNode*)get_page(pool, pool->file.length);
                    }
                    release(pool, 0);
                    new_nonleaf_node->n = DEGREE;
                    new_nonleaf_node->leaf = 0;
                    new_nonleaf_node->next = -1;
                    new_node newnode;
                    int child_pos = 0;
                    while (child_pos != 2 * DEGREE && (*cmp)(newchild.node_key, node->row_ptr[child_pos]) >= 0) {
                        child_pos++;
                    }

                    if (child_pos < DEGREE) {
                        newnode.node_key = node->row_ptr[DEGREE - 1];
                        newnode.node_ptr = new_nonleaf_node_addr;
                        for (int i = 0; i != DEGREE; i++) {
                            new_nonleaf_node->row_ptr[i] = node->row_ptr[DEGREE + i];
                            new_nonleaf_node->child[i] = node->child[DEGREE + i];
                        }
                        new_nonleaf_node->child[DEGREE] = node->child[2 * DEGREE];
                        for (int i = DEGREE - 1; i > child_pos; i--) {
                            node->row_ptr[i] = node->row_ptr[i - 1];
                            node->child[i + 1] = node->child[i];
                        }
                        node->row_ptr[child_pos] = newchild.node_key;
                        node->child[child_pos + 1] = newchild.node_ptr;
                    }
                    else if (child_pos == DEGREE) {
                        newnode.node_key = newchild.node_key;
                        newnode.node_ptr = new_nonleaf_node_addr;
                        for (int i = 0; i != DEGREE; i++) {
                            new_nonleaf_node->row_ptr[i] = node->row_ptr[DEGREE + i];
                            new_nonleaf_node->child[i + 1] = node->child[DEGREE + i + 1];
                        }
                        new_nonleaf_node->child[0] = newchild.node_ptr;
                    }
                    else {
                        newnode.node_key = node->row_ptr[DEGREE];
                        newnode.node_ptr = new_nonleaf_node_addr;
                        new_nonleaf_node->child[0] = node->child[DEGREE + 1];
                        for (int i = 0; i != child_pos - DEGREE - 1; i++) {
                            new_nonleaf_node->row_ptr[i] = node->row_ptr[DEGREE + i + 1];
                            new_nonleaf_node->child[i + 1] = node->child[DEGREE + i + 2];
                        }
                        new_nonleaf_node->row_ptr[child_pos - DEGREE - 1] = newchild.node_key;
                        new_nonleaf_node->child[child_pos - DEGREE] = newchild.node_ptr;
                        for (int i = child_pos - DEGREE; i != DEGREE; i++) {
                            new_nonleaf_node->row_ptr[i] = node->row_ptr[DEGREE + i];
                            new_nonleaf_node->child[i + 1] = node->child[DEGREE + i + 1];
                        }
                    }
                    node->n = DEGREE;
                    release(pool, addr);
                    release(pool, new_nonleaf_node_addr);
                    new_root_node->leaf = 0;
                    new_root_node->n = 1;
                    new_root_node->child[0] = addr;
                    new_root_node->child[1] = new_nonleaf_node_addr;
                    new_root_node->next = -1;
                    new_root_node->row_ptr[0] = newnode.node_key;
                    release(pool, new_root_node_addr);
                    bctrl = (BCtrlBlock*)get_page(pool, 0);
                    bctrl->root_node = new_root_node_addr;
                    release(pool, 0);
                    return newnode;
                }
                else {
                    off_t new_nonleaf_node_addr;
                    BNode *new_nonleaf_node;
                    BCtrlBlock *bctrl = (BCtrlBlock*)get_page(pool, 0);
                    if (bctrl->free_node_head != -1) {
                        new_nonleaf_node_addr = bctrl->free_node_head;
                        new_nonleaf_node = (BNode*)get_page(pool, new_nonleaf_node_addr);
                        bctrl->free_node_head = new_nonleaf_node->next;
                    }
                    else {
                        new_nonleaf_node_addr = pool->file.length;
                        new_nonleaf_node = (BNode*)get_page(pool, pool->file.length);
                    }
                    release(pool, 0);
                    new_nonleaf_node->n = DEGREE;
                    new_nonleaf_node->leaf = 0;
                    new_nonleaf_node->next = -1;
                    new_node newnode;
                    int child_pos = 0;
                    while (child_pos != 2 * DEGREE && (*cmp)(newchild.node_key, node->row_ptr[child_pos]) >= 0) {
                        child_pos++;
                    }
                    if (child_pos < DEGREE) {
                        newnode.node_key = node->row_ptr[DEGREE - 1];
                        newnode.node_ptr = new_nonleaf_node_addr;
                        for (int i = 0; i != DEGREE; i++) {
                            new_nonleaf_node->row_ptr[i] = node->row_ptr[DEGREE + i];
                            new_nonleaf_node->child[i] = node->child[DEGREE + i];
                        }
                        new_nonleaf_node->child[DEGREE] = node->child[2 * DEGREE];
                        for (int i = DEGREE - 1; i > child_pos; i--) {
                            node->row_ptr[i] = node->row_ptr[i - 1];
                            node->child[i + 1] = node->child[i];
                        }
                        node->row_ptr[child_pos] = newchild.node_key;
                        node->child[child_pos + 1] = newchild.node_ptr;
                    }
                    else if (child_pos == DEGREE) {
                        newnode.node_key = newchild.node_key;
                        newnode.node_ptr = new_nonleaf_node_addr;
                        for (int i = 0; i != DEGREE; i++) {
                            new_nonleaf_node->row_ptr[i] = node->row_ptr[DEGREE + i];
                            new_nonleaf_node->child[i + 1] = node->child[DEGREE + i + 1];
                        }
                        new_nonleaf_node->child[0] = newchild.node_ptr;
                    }
                    else {
                        newnode.node_key = node->row_ptr[DEGREE];
                        newnode.node_ptr = new_nonleaf_node_addr;
                        new_nonleaf_node->child[0] = node->child[DEGREE + 1];
                        for (int i = 0; i != child_pos - DEGREE - 1; i++) {
                            new_nonleaf_node->row_ptr[i] = node->row_ptr[DEGREE + i + 1];
                            new_nonleaf_node->child[i + 1] = node->child[DEGREE + i + 2];
                        }
                        new_nonleaf_node->row_ptr[child_pos - DEGREE - 1] = newchild.node_key;
                        new_nonleaf_node->child[child_pos - DEGREE] = newchild.node_ptr;
                        for (int i = child_pos - DEGREE; i != DEGREE; i++) {
                            new_nonleaf_node->row_ptr[i] = node->row_ptr[DEGREE + i];
                            new_nonleaf_node->child[i + 1] = node->child[DEGREE + i + 1];
                        }
                    }
                    node->n = DEGREE;
                    release(pool, addr);
                    release(pool, new_nonleaf_node_addr);
                    return newnode;
                }
            }
        }
    }
}

void delete_from_node(BNode *node, int idx_delete) {
    for (int i = idx_delete; i < node->n - 1; i++) {
        node->row_ptr[i] = node->row_ptr[i + 1];
    }
    node->n--;
}

void merge_nodes(BufferPool *pool, BNode *parent_node, int ptr_pos, BNode *left_node, BNode *right_node, off_t left_addr, off_t right_addr) {
    left_node->row_ptr[left_node->n] = parent_node->row_ptr[ptr_pos];
    left_node->child[left_node->n + 1] = right_node->child[0];
    left_node->n++;
    for (int i = 0; i < right_node->n; i++) {
        left_node->row_ptr[left_node->n + i] = right_node->row_ptr[i];
        left_node->child[left_node->n + i + 1] = right_node->child[i + 1];
    }
    left_node->n += right_node->n;
    release(pool, left_addr);
    BCtrlBlock *bctrl = (BCtrlBlock*)get_page(pool, 0);
    right_node->n = 0;
    right_node->next = bctrl->free_node_head;
    bctrl->free_node_head = right_addr;
    release(pool, 0);
    release(pool, right_addr);
}

int delete_from_Btree(BufferPool *pool, off_t addr, off_t parent_addr, RID rid, 
                      b_tree_row_row_cmp_t cmp, b_tree_insert_nonleaf_handler_t insert_handler, 
                      b_tree_delete_nonleaf_handler_t delete_handler) {
    BNode *node = (BNode*)get_page(pool, addr);

    if (node->leaf) {
        int idx_delete = 0;
        for(; idx_delete < node->n; idx_delete++) {
            if((*cmp)(rid, node->row_ptr[idx_delete]) == 0) {
                break;
            }
        }

        if (idx_delete < node->n) {
            delete_from_node(node, idx_delete);

            if (node->n == 0 && parent_addr == -1) { 
                BCtrlBlock *bctrl = (BCtrlBlock*)get_page(pool, 0);
                node->next = bctrl->free_node_head;
                bctrl->free_node_head = addr;
                bctrl->root_node = -1;
                release(pool, 0);
            }
        }

        release(pool, addr);
        return -1;
    } else {
        off_t child_node_addr;
        int child_node_pos = 0;
        while (child_node_pos < node->n && (*cmp)(rid, node->row_ptr[child_node_pos]) >= 0) {
            child_node_pos++;
        }
        child_node_addr = node->child[child_node_pos];
        release(pool, addr);

        int del_child = delete_from_Btree(pool, child_node_addr, addr, rid, cmp, insert_handler, delete_handler);
        node = (BNode*)get_page(pool, addr);

        if (del_child == -1) {
            release(pool, addr);
            return -1;
        }

        BNode *childnode = (BNode*)get_page(pool, node->child[0]);
        if (node->n > DEGREE) {
            if (childnode->leaf) {
                (*delete_handler)(node->row_ptr[del_child]); 
            }
            release(pool, node->child[0]);
            delete_from_node(node, del_child);
            release(pool, addr);
            return -1;
        } else {
            BNode *parent_node = (BNode*)get_page(pool, parent_addr);
            int ptr_pos = 0;
            while (ptr_pos < parent_node->n + 1 && parent_node->child[ptr_pos] != addr) {
                ptr_pos++;
            }

            off_t f_child_addr;
            BNode *child_page;
            if (ptr_pos != parent_node->n) {
                f_child_addr = parent_node->child[ptr_pos + 1];
                child_page = (BNode*)get_page(pool, f_child_addr);

                if (child_page->n > DEGREE) {
                    if (childnode->leaf) {
                        (*delete_handler)(node->row_ptr[del_child]); 
                    }
                    release(pool, node->child[0]);
                    delete_from_node(node, del_child);
                    node->row_ptr[node->n] = parent_node->row_ptr[ptr_pos];
                    node->child[node->n + 1] = child_page->child[0];
                    node->n++;
                    parent_node->row_ptr[ptr_pos] = child_page->row_ptr[0];
                    delete_from_node(child_page, 0);
                    release(pool, addr);
                    release(pool, parent_addr);
                    release(pool, f_child_addr);
                    return -1;
                } else {
                    merge_nodes(pool, parent_node, ptr_pos, node, child_page, addr, f_child_addr);
                    release(pool, parent_addr);
                    return ptr_pos;
                }
            } else {
                f_child_addr = parent_node->child[ptr_pos - 1];
                child_page = (BNode*)get_page(pool, f_child_addr);

                if (child_page->n > DEGREE) {
                    if (childnode->leaf) {
                        (*delete_handler)(node->row_ptr[del_child]); 
                    }
                    release(pool, node->child[0]);
                    delete_from_node(node, del_child);
                    for (size_t i = node->n; i > 0; i--) {
                        node->row_ptr[i] = node->row_ptr[i - 1];
                        node->child[i + 1] = node->child[i];
                    }
                    node->child[1] = node->child[0];
                    node->n++;
                    node->row_ptr[0] = parent_node->row_ptr[ptr_pos - 1];
                    node->child[0] = child_page->child[child_page->n];
                    parent_node->row_ptr[ptr_pos - 1] = child_page->row_ptr[child_page->n - 1];
                    child_page->n--;
                    release(pool, addr);
                    release(pool, parent_addr);
                    release(pool, f_child_addr);
                    return -1;
                } else {
                    merge_nodes(pool, parent_node, ptr_pos - 1, child_page, node, f_child_addr, addr);
                    release(pool, parent_addr);
                    return ptr_pos - 1;
                }
            }
        }
    }
}


void b_tree_init(const char *filename, BufferPool *pool) {
    /* TODO: add code here */
    init_buffer_pool(filename, pool);
    if (pool->file.length != 0) {
        return;
    }
    BCtrlBlock *bctrl = (BCtrlBlock*)get_page(pool, 0);
    bctrl->free_node_head = -1;
    bctrl->root_node = -1;
    release(pool, 0);
}
void b_tree_close(BufferPool *pool) {
    close_buffer_pool(pool);
}
RID b_tree_search(BufferPool *pool, void *key, size_t size, b_tree_ptr_row_cmp_t cmp) {
    BCtrlBlock *bctrl = (BCtrlBlock*)get_page(pool, 0);
    off_t root_node_addr = bctrl->root_node;
    release(pool, 0);
    if (root_node_addr == -1) {
        RID ans;
        get_rid_block_addr(ans) = -1;
        get_rid_idx(ans) = 0;
        return ans;
    }
    BNode *node = (BNode*)get_page(pool, root_node_addr);
    off_t addr = root_node_addr;
    while (!node->leaf) {
        int child_pos = 0;
        while (child_pos != node->n && (*cmp)(key, size, node->row_ptr[child_pos]) >= 0) {
            child_pos++;
        }

        off_t child_addr = node->child[child_pos];
        release(pool, addr);
        addr = child_addr;
        node = (BNode*)get_page(pool, addr);
    }
    for (int i = 0; i != node->n; i++) {
        if ((*cmp)(key, size, node->row_ptr[i]) == 0) {
            RID ans = node->row_ptr[i];
            release(pool, addr);
            return ans;
        }
    }
    release(pool, addr);
    RID ans;
    get_rid_block_addr(ans) = -1;
    get_rid_idx(ans) = 0;
    return ans;
}
RID b_tree_insert(BufferPool *pool, RID rid, b_tree_row_row_cmp_t cmp, b_tree_insert_nonleaf_handler_t insert_handler) {
    BCtrlBlock *bctrl = (BCtrlBlock*)get_page(pool, 0);
    off_t root_node_addr = bctrl->root_node;
    BNode *root;
    if (root_node_addr == -1) {
        if (bctrl->free_node_head != -1) {
            root = (BNode*)get_page(pool, bctrl->free_node_head);
            bctrl->free_node_head = root->next;
        }
        else {
            root = (BNode*)get_page(pool, PAGE_SIZE);
        }
        root->n = 0;
        root->next = -1;
        root->leaf = 1;
        bctrl->root_node = PAGE_SIZE;
    }
    else {
        root = (BNode*)get_page(pool, root_node_addr);
    }
    root_node_addr = bctrl->root_node;
    release(pool, 0);
    release(pool, root_node_addr);
    insert_to_Btree(pool, root_node_addr, rid, cmp, root_node_addr, insert_handler);
    return rid;
}
void b_tree_delete(BufferPool *pool, RID rid, b_tree_row_row_cmp_t cmp, b_tree_insert_nonleaf_handler_t insert_handler, b_tree_delete_nonleaf_handler_t delete_handler) {
    BCtrlBlock *bctrl = (BCtrlBlock*)get_page(pool, 0);
    off_t root_node_addr = bctrl->root_node;
    if (root_node_addr == -1) {
        return;
    }
    release(pool, 0);
    delete_from_Btree(pool, root_node_addr, -1, rid, cmp, insert_handler, delete_handler);
}