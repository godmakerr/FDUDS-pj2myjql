#include "str.h"

#include "table.h"

void read_string(Table *table, RID rid, StringRecord *record) {
    table_read(table, rid, (ItemPtr)&(record->chunk));
    record->idx = 0;
}

int has_next_char(StringRecord *record) {
    if (get_str_chunk_size(&(record->chunk)) != record->idx) {
        return 1;
    }
    else {
        if (get_rid_idx(get_str_chunk_rid(&(record->chunk))) == -1) {
            return 0;
        }
        else {
            return 1;
        }
    }
}

char next_char(Table *table, StringRecord *record) {
    if (get_str_chunk_size(&(record->chunk)) != record->idx) {
        return get_str_chunk_data_ptr(&(record->chunk))[record->idx++];
    }
    else {
        RID rid = get_str_chunk_rid(&(record->chunk));
        if (get_rid_idx(rid) == -1) {
            printf("next char doesn't exists\n");
            return 0;
        }
        else {
            table_read(table, rid, (ItemPtr)&(record->chunk));
            record->idx = 0;
            return get_str_chunk_data_ptr(&(record->chunk))[record->idx++];
        }
    }
}

int compare_string_record(Table *table, const StringRecord *a, const StringRecord *b) {
    StringRecord recA = *a;
    StringRecord recB = *b;
    size_t sizeA = get_str_chunk_size(&(recA.chunk));
    size_t sizeB = get_str_chunk_size(&(recB.chunk));
    if (sizeA == 0) {
        return -1;
    }
    if (sizeB == 0) {
        return 1;
    }
    while (has_next_char(&recA) && has_next_char(&recB)) {
        char a = next_char(table, &recA);
        char b = next_char(table, &recB);
        if (a != b) return a > b ? 1 : -1;
    }
    if (has_next_char(&recB)) {
        return -1;
    }
    if (has_next_char(&recA)) {
        return 1;
    }
    return 0;
}

RID write_string(Table *table, const char *data, off_t size) {

    short last_chunk_size = size % (STR_CHUNK_MAX_SIZE - sizeof(RID) - sizeof(short));
    if (last_chunk_size == 0) {
        last_chunk_size = (STR_CHUNK_MAX_SIZE - sizeof(RID) - sizeof(short));
    }

    StringChunk last_chunk;
    RID last_rid;
    get_rid_block_addr(last_rid) = -1;
    get_rid_idx(last_rid) = -1;
    get_str_chunk_rid(&last_chunk) = last_rid;
    get_str_chunk_size(&last_chunk) = last_chunk_size;

    memcpy(get_str_chunk_data_ptr(&last_chunk), data + size - last_chunk_size, last_chunk_size);

    RID rid = table_insert(table, (ItemPtr)&last_chunk, calc_str_chunk_size(last_chunk_size));


    short chunk_data_size = STR_CHUNK_MAX_SIZE - sizeof(RID) - sizeof(short);
    short full_chunk_num = (short)((size - last_chunk_size) / chunk_data_size);


    for (int chunk_iter = full_chunk_num; chunk_iter > 0; chunk_iter--) {
        StringChunk chunk;
        get_str_chunk_rid(&chunk) = rid;
        get_str_chunk_size(&chunk) = chunk_data_size;

        memcpy(get_str_chunk_data_ptr(&chunk), data + (chunk_iter - 1) * chunk_data_size, chunk_data_size);

        rid = table_insert(table, (ItemPtr)&chunk, sizeof(chunk));
    }

    return rid;
}


void delete_string(Table *table, RID rid) {
    while (get_rid_idx(rid) != -1) {
        StringChunk chunk;
        table_read(table, rid, (ItemPtr)&chunk);
        table_delete(table, rid);
        rid = get_str_chunk_rid(&chunk);
    }
}

/* void print_string(Table *table, const StringRecord *record) {
    StringRecord rec = *record;
    printf("\"");
    while (has_next_char(&rec)) {
        printf("%c", next_char(table, &rec));
    }
    printf("\"");
} */

size_t load_string(Table *table, const StringRecord *record, char *dest, size_t max_size) {
    StringRecord rec = *record;
    size_t size = 0;
    while (has_next_char(&rec) && size < max_size) {
        dest[size++] = next_char(table, &rec);
    }
    if (size < max_size)    dest[size] = 0;
    return size;
}

/* void chunk_printer(ItemPtr item, short item_size) {
    if (item == NULL) {
        printf("NULL");
        return;
    }
    StringChunk *chunk = (StringChunk*)item;
    short size = get_str_chunk_size(chunk), i;
    printf("StringChunk(");
    print_rid(get_str_chunk_rid(chunk));
    printf(", %d, \"", size);
    for (i = 0; i < size; i++) {
        printf("%c", get_str_chunk_data_ptr(chunk)[i]);
    }
    printf("\")");
} */