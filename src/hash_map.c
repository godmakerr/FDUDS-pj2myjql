#include "hash_map.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void hash_table_init(const char *filename, BufferPool *pool, off_t n_directory_blocks) {
    init_buffer_pool(filename, pool);
    if (pool->file.length != 0) {
        return;
    }
    HashMapControlBlock *ctrl = (HashMapControlBlock*)get_page(pool, 0);
    ctrl->n_directory_blocks = n_directory_blocks;
    ctrl->free_block_head = -1;
    ctrl->max_size = (n_directory_blocks + 1) * PAGE_SIZE;
    release(pool, 0);
    for (int i = 0; i != n_directory_blocks; i++) {
        HashMapDirectoryBlock *dir_block = (HashMapDirectoryBlock*)get_page(pool, PAGE_SIZE * (i + 1));
        memset(dir_block->directory, -1, PAGE_SIZE);
        release(pool, PAGE_SIZE * (i + 1));
    }
}

void hash_table_close(BufferPool *pool) {
    close_buffer_pool(pool);
}

void hash_table_insert(BufferPool *pool, short size, off_t addr) {
    off_t dir_index = size / HASH_MAP_DIR_BLOCK_SIZE;
    off_t dir_offset = size % HASH_MAP_DIR_BLOCK_SIZE;
    off_t dir_page_num = (dir_index + 1) * PAGE_SIZE;

    HashMapDirectoryBlock *dir_block = (HashMapDirectoryBlock*)get_page(pool, dir_page_num);
    off_t now_page_addr = dir_block->directory[dir_offset];
    release(pool, dir_page_num);

    if (now_page_addr == -1) {
        HashMapControlBlock *ctrl = (HashMapControlBlock*)get_page(pool, 0);
        if (ctrl->free_block_head != -1) {
            dir_block = (HashMapDirectoryBlock*)get_page(pool, dir_page_num);
            dir_block->directory[dir_offset] = ctrl->free_block_head;
            release(pool, dir_page_num);

            HashMapBlock *fst_free_block = (HashMapBlock*)get_page(pool, ctrl->free_block_head);
            off_t nxt_free_block = fst_free_block->next;
            fst_free_block->next = -1;
            fst_free_block->n_items = 1;
            fst_free_block->table[0] = addr;
            release(pool, ctrl->free_block_head);

            ctrl->free_block_head = nxt_free_block;
            release(pool, 0);
            return;
        } else {
            off_t max_size = ctrl->max_size;
            dir_block = (HashMapDirectoryBlock*)get_page(pool, dir_page_num);
            dir_block->directory[dir_offset] = max_size;
            release(pool, dir_page_num);

            HashMapBlock *new_block = (HashMapBlock*)get_page(pool, max_size);
            new_block->n_items = 1;
            new_block->table[0] = addr;
            new_block->next = -1;
            release(pool, max_size);

            ctrl->max_size += PAGE_SIZE;
            release(pool, 0);
            return;
        }
    } else {
        HashMapBlock *blk = (HashMapBlock*)get_page(pool, now_page_addr);
        if (blk->n_items != HASH_MAP_BLOCK_SIZE) {
            blk->table[blk->n_items] = addr;
            blk->n_items++;
            release(pool, now_page_addr);
            return;
        } else {
            HashMapControlBlock *ctrl = (HashMapControlBlock*)get_page(pool, 0);
            off_t max_size = ctrl->max_size;
            release(pool, now_page_addr);

            HashMapBlock *new_block = (HashMapBlock*)get_page(pool, max_size);
            new_block->next = now_page_addr;
            new_block->n_items = 1;
            new_block->table[0] = addr;
            release(pool, max_size);

            dir_block = (HashMapDirectoryBlock*)get_page(pool, dir_page_num);
            dir_block->directory[dir_offset] = max_size;
            release(pool, dir_page_num);

            ctrl->max_size += PAGE_SIZE;
            release(pool, 0);
            return;
        }
    }
}

off_t hash_table_pop_lower_bound(BufferPool *pool, short size) {
    HashMapControlBlock *ctrl = (HashMapControlBlock*)get_page(pool, 0);
    off_t n_directory_blocks = ctrl->n_directory_blocks;
    release(pool, 0);

    off_t dir_index = size / HASH_MAP_DIR_BLOCK_SIZE;
    off_t dir_offset = size % HASH_MAP_DIR_BLOCK_SIZE;
    off_t dir_page_num = (dir_index + 1) * PAGE_SIZE;
    HashMapDirectoryBlock *dir_block = (HashMapDirectoryBlock*)get_page(pool, dir_page_num);

    while (dir_block->directory[dir_offset] == -1 && (dir_page_num / PAGE_SIZE - 1) * HASH_MAP_DIR_BLOCK_SIZE + dir_offset < n_directory_blocks * HASH_MAP_DIR_BLOCK_SIZE) {
        release(pool, dir_page_num);
        if (++dir_offset == HASH_MAP_DIR_BLOCK_SIZE) {
            dir_offset = 0;
            dir_page_num += PAGE_SIZE;
        }
        dir_block = (HashMapDirectoryBlock*)get_page(pool, dir_page_num);
    }

    off_t addr = -1;
    if ((dir_page_num / PAGE_SIZE - 1) * HASH_MAP_DIR_BLOCK_SIZE + dir_offset < n_directory_blocks * HASH_MAP_DIR_BLOCK_SIZE) {
        off_t map_block_addr = dir_block->directory[dir_offset];
        release(pool, dir_page_num);

        HashMapBlock *map_block = (HashMapBlock*)get_page(pool, map_block_addr);
        addr = map_block->table[0];
        release(pool, map_block_addr);

        hash_table_pop(pool, (short)((dir_page_num / PAGE_SIZE - 1) * HASH_MAP_DIR_BLOCK_SIZE + dir_offset), addr);
    } else {
        release(pool, dir_page_num);
    }
    return addr;
}

void hash_table_pop(BufferPool *pool, short size, off_t addr) {
    off_t dir_index = size / HASH_MAP_DIR_BLOCK_SIZE;
    off_t dir_offset = size % HASH_MAP_DIR_BLOCK_SIZE;
    off_t dir_page_num = (dir_index + 1) * PAGE_SIZE;

    HashMapDirectoryBlock *dir_block = (HashMapDirectoryBlock*)get_page(pool, dir_page_num);
    off_t now_page = dir_block->directory[dir_offset];
    release(pool, dir_page_num);

    off_t frn_page = now_page;
    while (1) {
        HashMapBlock *now_block = (HashMapBlock*)get_page(pool, now_page);
        off_t flag = -1;
        off_t nxt_page = now_block->next;

        for (off_t i = 0; i != now_block->n_items; i++) {
            if (now_block->table[i] == addr) {
                flag = i;
                break;
            }
        }

        if (flag == -1) {
            release(pool, now_page);
            frn_page = now_page;
            now_page = nxt_page;
        } else {
            off_t n_items = now_block->n_items;
            if (n_items == 1) {
                if (frn_page == now_page) {
                    dir_block = (HashMapDirectoryBlock*)get_page(pool, dir_page_num);
                    dir_block->directory[dir_offset] = now_block->next;
                    release(pool, dir_page_num);
                } else {
                    HashMapBlock *frn_block = (HashMapBlock*)get_page(pool, frn_page);
                    frn_block->next = now_block->next;
                    release(pool, frn_page);
                }

                now_block->n_items = 0;
                HashMapControlBlock *ctrl = (HashMapControlBlock*)get_page(pool, 0);
                now_block->next = ctrl->free_block_head;
                release(pool, now_page);
                ctrl->free_block_head = now_page;
                release(pool, 0);
                return;
            } else {
                for (off_t i = flag; i < n_items - 1; i++) {
                    now_block->table[i] = now_block->table[i + 1];
                }
                now_block->n_items--;
                release(pool, now_page);
                return;
            }
        }
    }
}

/* void print_hash_table(BufferPool *pool) {
    HashMapControlBlock *ctrl = (HashMapControlBlock*)get_page(pool, 0);
    HashMapDirectoryBlock *dir_block;
    off_t block_addr, next_addr;
    HashMapBlock *block;
    int i, j;
    printf("----------HASH TABLE----------\n");
    for (i = 0; i < ctrl->max_size; ++i) {
        dir_block = (HashMapDirectoryBlock*)get_page(pool, (i / HASH_MAP_DIR_BLOCK_SIZE + 1) * PAGE_SIZE);
        if (dir_block->directory[i % HASH_MAP_DIR_BLOCK_SIZE] != 0) {
            printf("%d:", i);
            block_addr = dir_block->directory[i % HASH_MAP_DIR_BLOCK_SIZE];
            while (block_addr != 0) {
                block = (HashMapBlock*)get_page(pool, block_addr);
                printf("  [" FORMAT_OFF_T "]", block_addr);
                printf("{");
                for (j = 0; j < block->n_items; ++j) {
                    if (j != 0) {
                        printf(", ");
                    }
                    printf(FORMAT_OFF_T, block->table[j]);
                }
                printf("}");
                next_addr = block->next;
                release(pool, block_addr);
                block_addr = next_addr;
            }
            printf("\n");
        }
        release(pool, (i / HASH_MAP_DIR_BLOCK_SIZE + 1) * PAGE_SIZE);
    }
    release(pool, 0);
    printf("------------------------------\n");
} */