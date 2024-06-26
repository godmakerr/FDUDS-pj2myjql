#include "myjql.h"

#include "buffer_pool.h"
#include "b_tree.h"
#include "table.h"
#include "str.h"
#include <stdlib.h>

typedef struct {
    RID key;
    RID value;
} Record;

void read_record(Table *table, RID rid, Record *record) {
    table_read(table, rid, (ItemPtr)record);
}
RID write_record(Table *table, const Record *record) {
    return table_insert(table, (ItemPtr)record, sizeof(Record));
}
void delete_record(Table *table, RID rid) {
    table_delete(table, rid);
}


BufferPool bp_idx;
Table tbl_rec;
Table tbl_str;


int compare_key_with_record(void *key, size_t key_len, RID rid) {
    Record record;
    read_record(&tbl_rec, rid, &record);
    StringRecord strrec;
    read_string(&tbl_str, record.key, &strrec);
    if (get_str_chunk_size(&(strrec.chunk)) == 0) {
        return 1;
    }
    size_t charptr = 0;
    while (has_next_char(&strrec) && charptr != key_len) {
        char a = ((char*)key)[charptr++];
        char b = next_char(&tbl_str, &strrec);
        if (a != b) { 
            return a > b ? 1 : -1;
        }
    }
    if (charptr == key_len && has_next_char(&strrec)) {
        return -1;
    }
    if (charptr != key_len && !has_next_char(&strrec)) {
        return 1;
    }
    return 0;
}
int compare_records(RID rid1, RID rid2) {
    Record rec1;
    read_record(&tbl_rec, rid1, &rec1);
    StringRecord strrec1;
    read_string(&tbl_str, rec1.key, &strrec1);
    Record rec2;
    read_record(&tbl_rec, rid2, &rec2);
    StringRecord strrec2;
    read_string(&tbl_str, rec2.key, &strrec2);
    if (get_str_chunk_size(&(strrec1.chunk)) == 0) {
        return (get_str_chunk_size(&(strrec2.chunk)) == 0) ? 0 : -1;
    }
    if (get_str_chunk_size(&(strrec2.chunk)) == 0) {
        return 1;
    }
    while (has_next_char(&strrec1) && has_next_char(&strrec2)) {
        char a = next_char(&tbl_str, &strrec1);
        char b = next_char(&tbl_str, &strrec2);
        if (a != b) {
            return a > b ? 1 : -1;
        }
    }
    if (!has_next_char(&strrec1) && has_next_char(&strrec2)) {
        return -1;
    }
    if (has_next_char(&strrec1) && !has_next_char(&strrec2)) {
        return 1;
    }
    return 0;
}

typedef struct {
    struct InsertTemp *next;
    RID chunk;
} InsertTemp;

RID handle_insert(RID rid) {
    Record record;
    read_record(&tbl_rec, rid, &record);
    RID chunkrid = record.key;
    InsertTemp *head = NULL;
    while (get_rid_block_addr(chunkrid) != -1) {
        InsertTemp *nd = (InsertTemp*)malloc(sizeof(InsertTemp));
        nd->chunk = chunkrid;
        nd->next = head;
        head = nd;
        StringChunk tempchunk;
        table_read(&tbl_str, chunkrid, &tempchunk);
        chunkrid = get_str_chunk_rid(&tempchunk);
        if (get_rid_block_addr(chunkrid) == -1) {
            break;
        }
    }
    InsertTemp *readptr = head;
    StringChunk newchunk;
    table_read(&tbl_str, head->chunk, &newchunk);
    short size;
    size = calc_str_chunk_size(get_str_chunk_size(&newchunk));
    RID nextchunk;
    while (readptr != NULL) {
        nextchunk = table_insert(&tbl_str, &newchunk, size);
        head = readptr->next;
        free(readptr);
        readptr = head;
        if (readptr != NULL) {
            table_read(&tbl_str, head->chunk, &newchunk);
            get_str_chunk_rid(&newchunk) = nextchunk;
            size = sizeof(newchunk);
        }
    }
    Record newrec;
    newrec.key = nextchunk;
    get_rid_block_addr(newrec.value) = -1;
    get_rid_idx(newrec.value) = -1;
    RID newrid;
    newrid = write_record(&tbl_rec, &newrec);
    return newrid;
}
void handle_delete(RID rid) {
    Record record;
    read_record(&tbl_rec, rid, &record);
    delete_string(&tbl_str, record.key);
    delete_record(&tbl_rec, rid);
    return;
}

void myjql_init() {
    b_tree_init("record.idx", &bp_idx);
    table_init(&tbl_rec, "record.data", "record.fsm");
    table_init(&tbl_str, "str.data", "str.fsm");
}
void myjql_close() {
    /* validate_buffer_pool(&bp_idx);
    validate_buffer_pool(&tbl_rec.data_pool);
    validate_buffer_pool(&tbl_rec.fsm_pool);
    validate_buffer_pool(&tbl_str.data_pool);
    validate_buffer_pool(&tbl_str.fsm_pool); */
    b_tree_close(&bp_idx);
    table_close(&tbl_rec);
    table_close(&tbl_str);
}
size_t myjql_get(const char *key, size_t key_len, char *value, size_t max_size) {
    RID kvp = b_tree_search(&bp_idx, key, key_len, &compare_key_with_record);
    if (get_rid_block_addr(kvp) == -1 && get_rid_idx(kvp) == 0) {
        return -1;
    }
    Record record;
    read_record(&tbl_rec, kvp, &record);
    StringRecord string_record;
    read_string(&tbl_str, record.value, &string_record);
    return load_string(&tbl_str, &string_record, value, max_size);
}
void myjql_set(const char *key, size_t key_len, const char *value, size_t value_len) {
    RID kvp = b_tree_search(&bp_idx, key, key_len, &compare_key_with_record);
    if (get_rid_block_addr(kvp) == -1 && get_rid_idx(kvp) == 0) {
        RID key_rid, value_rid;
        key_rid = write_string(&tbl_str, key, key_len);
        value_rid = write_string(&tbl_str, value, value_len);
        Record record;
        record.key = key_rid;
        record.value = value_rid;
        RID write_rid;
        write_rid = write_record(&tbl_rec, &record);
        b_tree_insert(&bp_idx, write_rid, &compare_records, &handle_insert);
        return;
    }
    else {
        Record record;
        read_record(&tbl_rec, kvp, &record);
        delete_string(&tbl_str, record.value);
        RID new_value_rid;
        new_value_rid = write_string(&tbl_str, value, value_len);
        b_tree_delete(&bp_idx, kvp, &compare_records, &handle_insert, &handle_delete);
        delete_record(&tbl_rec, kvp);
        record.value = new_value_rid;
        RID new_kvp = write_record(&tbl_rec, &record);
        b_tree_insert(&bp_idx, new_kvp, &compare_records, &handle_insert);
        return;
    }
}
void myjql_del(const char *key, size_t key_len) {
    RID kvp = b_tree_search(&bp_idx, key, key_len, &compare_key_with_record);
    if (get_rid_block_addr(kvp) == -1 && get_rid_idx(kvp) == 0) {
        return;
    }
    b_tree_delete(&bp_idx, kvp, &compare_records, &handle_insert, &handle_delete);
    Record record;
    read_record(&tbl_rec, kvp, &record);
    delete_string(&tbl_str, record.key);
    delete_string(&tbl_str, record.value);
    delete_record(&tbl_rec, kvp);
    return;
}
/* void myjql_analyze() {
    printf("Record Table:\n");
    analyze_table(&tbl_rec);
    printf("String Table:\n");
    analyze_table(&tbl_str);
} */