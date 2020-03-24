#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

typedef struct node{
  struct node_t *next;
  int value;
}node_t;

typedef struct bucket{
  int *array;
  int len;
}bucket_t;

typedef struct table{
  node_t **buckets;
}table_t;

typedef struct meta{
  table_t *table;
  bucket_t **arr;
  int start;
  int stop;
}meta_t;

static inline void print_arr(int *list, const int size){
  if(size == 1){
    printf("[%d]", list[0]);
    return;
  }
  for(int i=0; i<size; i++){
    if(i == 0){
      printf("[%d,", list[i]);
      continue;
    }
    if(i == size-1){
      printf("%d]", list[i]);
      return;
    }
    printf("%d,", list[i]);
  }
}

static inline void print_bucket(table_t *table, const int size){
  node_t **map = table->buckets;
  for(int i=0; i<size; i++){
    node_t *list = map[i];
    printf("[");
    while(list != NULL){
      if(list->next == NULL){
        printf("%d", list->value);
      } else {
        printf("%d,", list->value);
      }
      list = (node_t*)list->next;
    }
    printf("] : %d\n", i);
  }
}

static inline void destroy_arr(bucket_t **arr, const int len){
  for(int i=0; i<len; i++){
    free(arr[i]->array);
    free(arr[i]);
  }
  free(arr);
}

static inline void destroy_table(table_t *table, const int len){
  node_t *current;
  for(int i=0; i<len; i++){
    current = table->buckets[i];
    node_t *next;
    while(current != NULL){
      next = (node_t*)current->next;
      free(current);
      current = next;
    }
  }
  free(table->buckets);
  free(table);
}

/**
 * @param value, the linked list will contain this value
 * @return allocated node
 */
static inline node_t *create_node(const int value){
  node_t *node = calloc(1, sizeof(node_t));
  node->value = value;
  return node;
}

/**
 * Is used to calculate the amount of buckets the hash map will have,
 * depending on the max value of the unsorted array.
 * @param arr, to find the max value
 * @param size, size of array
 * @return the max value
 */
static inline int find_divider(int *arr, const int size){
  int max = 0;
  for(int i=0; i<size; i++){
    if(arr[i] >= max){
      max = arr[i];
    }
  }
  return max;
}

/**
 * Aux func for table_insert function
 * @param table, the table to be inserted
 * @param index, which bucket to insert
 * @param value, to insert
 */
static inline void insert(table_t *table, const int index, const int value){
  node_t *list = table->buckets[index];
  node_t *new_node = create_node(value);
  new_node->next = (struct node_t*)list;
  table->buckets[index] = new_node;
}

/**
 * @param amount the size of the table
 * @return
 */
static inline table_t *create_table(int amount){
  table_t *table = calloc(1, sizeof(bucket_t));
  table->buckets = calloc((size_t)amount, sizeof(node_t*));
  for(int i=0; i<amount; i++){
    node_t *list = calloc(1, sizeof(node_t));
    table->buckets[i] = list;
  }
  return table;
}

/**
 * @param table, to be inserted
 * @param arr, array with values to be inserted to the table
 * @param arr_size, size of the array
 * @param divider
 */
static inline void table_insert(table_t *table, int *arr, const int arr_size, const int divider){
  for(int i=0; i<arr_size; i++){
    insert(table, arr[i]/divider, arr[i]);
  }
}

/**
 * This function serves as a convertion function from linked list to array
 * @param list, the linked list to convert
 * @return a bucket with an array inside
 */
static inline bucket_t *conv(node_t *list){
  int len = -1;
  node_t *current = list;
  while(current != NULL){
    current = (node_t*)current->next;
    len++;
  }
  int *arr = calloc((size_t)len, sizeof(int));
  for(int i=0; i<len; i++){
    arr[i] = list->value;
    list = (node_t*)list->next;
  }
  bucket_t *bucket = calloc(1, sizeof(bucket_t));
  bucket->array = arr;
  bucket->len = len;
  return bucket;
}

/**
 * @param bucket, that includes the array which is going to be sorted and the size
 * @return bucket, which includes a sorted array and the size
 */
static inline bucket_t *insert_sort(bucket_t *bucket){
  if(!bucket->len){
    return bucket;
  }
  for(int i=0; i<bucket->len; i++){
    if(i+1 > bucket->len){
      return bucket;
    }
    int current = bucket->array[i];
    for(int j=i; j>-1; j--){
      if(current > bucket->array[j]){
        break;
      }
      if(current < bucket->array[j]){
        int tmp = bucket->array[j];
        bucket->array[j+1] = tmp;
        bucket->array[j] = current;
      }
    }
  }
  return bucket;
}

/**
 * @param arr, contains all the sorted arrays which is going to be joined together
 * @param size, amount of buckets to join
 * @param final_array, the final array which the buckets are being joined to
 */
static inline void join(bucket_t **arr, const int size, int *final_array){
  int next = 0;
  for(int i=0; i<size; i++){
    int *array = arr[i]->array;
    for(int j=next, k=0; j<next+arr[i]->len; j++, k++){
      final_array[j] = array[k];
    }
    next += arr[i]->len;
  }
}

/**
 * Sorts the buckets in parallel
 * @param args, the meta data so that the for loop could be divided into chunks
 * @return terminates the thread
 */
static inline void *sort_buckets(void *args){
  meta_t *meta = (meta_t*)args;
  for(int i=meta->start; i<meta->stop; i++){
    bucket_t *bucket = conv(meta->table->buckets[i]);
    if(bucket->len == 0){
      meta->arr[i] = NULL;
      continue;
    }
    meta->arr[i] = insert_sort(bucket);
  }
  free(meta);
  pthread_exit(NULL);
}

/**
 * The main function, calls all the other necessary functions
 * @param unsorted_list, the list to be sorted
 * @param unsort_len, len of the list
 * @param nr_threads, amount of threads to initialize
 * @param divisor, value that decides which bucket to insert
 * @return the final sorted array
 */
static inline bucket_t *bucket_sort(int *unsorted_list, int unsort_len, int nr_threads, int divisor){
  int max = find_divider(unsorted_list, unsort_len);
  int buckets = (max/divisor)+1;
  table_t *table = create_table(buckets);
  table_insert(table, unsorted_list, unsort_len, divisor);
  free(unsorted_list);
  bucket_t **arr = calloc((size_t)max+1, sizeof(int*));

  pthread_t threads[nr_threads];
  for(int i=0; i<nr_threads; i++){
    meta_t *meta = calloc(1, sizeof(meta_t));
    meta->start = i * buckets/nr_threads;
    meta->stop = (i + 1) * buckets/nr_threads;
    meta->table = table;
    meta->arr = arr;
    pthread_create(&threads[i], NULL, sort_buckets, meta);
  }
  for (int i = 0; i < nr_threads; i++) {
    pthread_join(threads[i], NULL);
  }

  destroy_table(table, buckets);

  bucket_t *final = calloc(1, sizeof(bucket_t));
  int *final_array = calloc((size_t)unsort_len, sizeof(int));
  final->array = final_array;
  final->len = unsort_len;

  join(arr,buckets,final_array);
  destroy_arr(arr,buckets);

  return final;
}

int main(const int argc, const char *argv[]) {
  if (argc != 4) {
    printf("Program takes 3 arguments!\n");
    return 1;
  }
  const int len_array = atoi(argv[1]);
  const int divisor = atoi(argv[2]);
  const int nr_threads = atoi(argv[3]);
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  int *unsorted_list = calloc((size_t)len_array, sizeof(int));
  for(int i=len_array; i>0; i--){
    unsorted_list[len_array-i] = i;
  }
  bucket_t *sorted_list = bucket_sort(unsorted_list, len_array, nr_threads, divisor);
  pthread_attr_destroy(&attr);
  free(sorted_list->array);
  free(sorted_list);
  return 0;
}
