#include "buffer_pool.h"
#include "file_io.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h> 

void init_buffer_pool(const char *filename, BufferPool *pool) {
	open_file(&pool->file, filename);
    for (int i = 0; i != CACHE_PAGE; i++){
        pool->addrs[i] = -1;
        pool->cnt[i] = -1;
        pool->ref[i] = 0;
        pool->last_accessed[i] = 0;
		pool->dirty[i] = 0; 
    }
}

void close_buffer_pool(BufferPool *pool) {
	for (int i = 0; i != CACHE_PAGE; i++) {
        if (pool->addrs[i] != -1) {
            write_page(&pool->pages[i], &pool->file, pool->addrs[i]);
        }
    }
    close_file(&pool->file);
}

Page *get_page(BufferPool *pool, off_t addr) {
	time_t current_time = time(NULL);

    for (int i = 0; i != CACHE_PAGE; i++){
        if (pool->addrs[i] == addr){
            pool->ref[i]++;
            pool->cnt[i]++;
            pool->last_accessed[i] = current_time;
            return &pool->pages[i];
        }
    }

    int free_i = -1;
    time_t oldest_time = current_time;

    for (int i = 0; i != CACHE_PAGE; i++) {
        if (pool->ref[i] == 0 && (free_i == -1 || pool->last_accessed[i] < oldest_time)) {
            free_i = i;
            oldest_time = pool->last_accessed[i];
        }
    }

    if (free_i == -1) {
        return NULL;
    } else {
        if (pool->cnt[free_i] != -1) {
            write_page(&pool->pages[free_i], &pool->file, pool->addrs[free_i]);
        }

        FileIOResult result = read_page(&pool->pages[free_i], &pool->file, addr);

        if (result == ADDR_OUT_OF_RANGE && addr >= 0) {
            pool->file.length = addr + PAGE_SIZE;
            memset(&pool->pages[free_i], 0, PAGE_SIZE);
        } else if (result != FILE_IO_SUCCESS) {
            return NULL;
        }

        pool->addrs[free_i] = addr;
        for (int i = 0; i != CACHE_PAGE; i++) {
            if (pool->cnt[i] != -1) pool->cnt[i] = 0;
        }
        pool->cnt[free_i] = 1;
        pool->ref[free_i] = 1;
        pool->last_accessed[free_i] = current_time;
		pool->dirty[free_i] = 1; 

        return &pool->pages[free_i];
    }
}

void release(BufferPool *pool, off_t addr) {
    for (int i = 0; i != CACHE_PAGE; i++) {
        if (pool->addrs[i] == addr) {
			if (pool->dirty[i]) {
                write_page(&pool->pages[i], &pool->file, addr);
                pool->dirty[i] = 0;  
            }
            if (pool->ref[i] > 0) {
                pool->ref[i]--;
            }
            return;
        }
    }
}

/* void print_buffer_pool(BufferPool *pool) {
} */

/* void validate_buffer_pool(BufferPool *pool) {
} */
