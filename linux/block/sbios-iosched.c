/*
 *  sbios i/o scheduler.
 *
 *  Copyright (C) 2002 Jens Axboe <axboe@kernel.dk>
 */
#include <linux/kernel.h>
#include <linux/fs.h>
#include <linux/blkdev.h>
#include <linux/elevator.h>
#include <linux/bio.h>
#include <linux/module.h>
#include <linux/slab.h>
#include <linux/init.h>
#include <linux/compiler.h>
#include <linux/rbtree.h>

/*
 * See Documentation/block/sbios-iosched.txt
 */
static const int read_expire = HZ / 2;  /* max time before a read is submitted. */
static const int write_expire = 5 * HZ; /* ditto for writes, these limits are SOFT! */
static const int writes_starved = 2;    /* max times reads can starve a write */
static const int fifo_batch = 16;       /* # of sequential requests treated as one
				     by the above parameters. For throughput. */

struct sbios_data {
	/*
	 * run time data
	 */

	/*
	 * requests (sbios_rq s) are present on both sort_list and fifo_list
	 */
	struct rb_root sort_list[2];	
	struct list_head fifo_list[2];

	/*
	 * next in sort order. read, write or both are NULL
	 */
	struct request *next_rq[2];
	unsigned int batching;		/* number of sequential requests made */
	unsigned int starved;		/* times reads have starved writes */

	/*
	 * settings that change how the i/o scheduler behaves
	 */
	int fifo_expire[2];
	int fifo_batch;
	int writes_starved;
	int front_merges;
};

static inline struct rb_root *
sbios_rb_root(struct sbios_data *dd, struct request *rq)
{
	return &dd->sort_list[rq_data_dir(rq)];
}

/*
 * get the request after `rq' in sector-sorted order
 */
static inline struct request *
sbios_latter_request(struct request *rq)
{
	//struct rb_node *node = rb_next(&rq->rb_node);
	struct rb_node *node ;
	
	int data_dir = rq_data_dir(rq);
	if (data_dir == READ)
		node = rb_next_read(&rq->rb_node);
	else
		node = rb_next(&rq->rb_node);


	if (node)
		return rb_entry_rq(node);

	return NULL;
}

static void
sbios_add_rq_rb(struct sbios_data *dd, struct request *rq)
{
	struct rb_root *root = sbios_rb_root(dd, rq);

	elv_rb_add(root, rq);
}

static inline void
sbios_del_rq_rb(struct sbios_data *dd, struct request *rq)
{
	const int data_dir = rq_data_dir(rq);

	if (dd->next_rq[data_dir] == rq)
		dd->next_rq[data_dir] = sbios_latter_request(rq);

	elv_rb_del(sbios_rb_root(dd, rq), rq);
}

/*
 * add rq to rbtree and fifo
 */
static void
sbios_add_request(struct request_queue *q, struct request *rq)
{
	struct sbios_data *dd = q->elevator->elevator_data;
	const int data_dir = rq_data_dir(rq);

	/*
	 * This may be a requeue of a write request that has locked its
	 * target zone. If it is the case, this releases the zone lock.
	 */
	blk_req_zone_write_unlock(rq);

	sbios_add_rq_rb(dd, rq);

	/*
	 * set expire time and add to fifo list
	 */
	rq->fifo_time = jiffies + dd->fifo_expire[data_dir];
	list_add_tail(&rq->queuelist, &dd->fifo_list[data_dir]);
}

/*
 * remove rq from rbtree and fifo.
 */
static void sbios_remove_request(struct request_queue *q, struct request *rq)
{
	struct sbios_data *dd = q->elevator->elevator_data;

	rq_fifo_clear(rq);
	sbios_del_rq_rb(dd, rq);
}

static enum elv_merge
sbios_merge(struct request_queue *q, struct request **req, struct bio *bio)
{
	struct sbios_data *dd = q->elevator->elevator_data;
	struct request *__rq;

	/*
	 * check for front merge
	 */
	if (dd->front_merges) {
		sector_t sector = bio_end_sector(bio);

		__rq = elv_rb_find(&dd->sort_list[bio_data_dir(bio)], sector);
		if (__rq) {
			BUG_ON(sector != blk_rq_pos(__rq));

			if (elv_bio_merge_ok(__rq, bio)) {
				*req = __rq;
				return ELEVATOR_FRONT_MERGE;
			}
		}
	}

	return ELEVATOR_NO_MERGE;
}

static void sbios_merged_request(struct request_queue *q,
				    struct request *req, enum elv_merge type)
{
	struct sbios_data *dd = q->elevator->elevator_data;

	/*
	 * if the merge was a front merge, we need to reposition request
	 */
	if (type == ELEVATOR_FRONT_MERGE) {
		elv_rb_del(sbios_rb_root(dd, req), req);
		sbios_add_rq_rb(dd, req);
	}
}

static void
sbios_merged_requests(struct request_queue *q, struct request *req,
			 struct request *next)
{
	/*
	 * if next expires before rq, assign its expire time to rq
	 * and move into next position (next will be deleted) in fifo
	 */
	if (!list_empty(&req->queuelist) && !list_empty(&next->queuelist)) {
		if (time_before((unsigned long)next->fifo_time,
				(unsigned long)req->fifo_time)) {
			list_move(&req->queuelist, &next->queuelist);
			req->fifo_time = next->fifo_time;
		}
	}

	/*
	 * kill knowledge of next, this one is a goner
	 */
	sbios_remove_request(q, next);
}

/*
 * move request from sort list to dispatch queue.
 */
static inline void
sbios_move_to_dispatch(struct sbios_data *dd, struct request *rq)
{
	struct request_queue *q = rq->q;

	/*
	 * For a zoned block device, write requests must write lock their
	 * target zone.
	 */
	blk_req_zone_write_lock(rq);

	sbios_remove_request(q, rq);
	elv_dispatch_add_tail(q, rq);
}

/*
 * move an entry to dispatch queue
 */
static void
sbios_move_request(struct sbios_data *dd, struct request *rq)
{
	const int data_dir = rq_data_dir(rq);

	dd->next_rq[READ] = NULL;
	dd->next_rq[WRITE] = NULL;
	dd->next_rq[data_dir] = sbios_latter_request(rq);

	/*
	 * take it off the sort and fifo list, move
	 * to dispatch queue
	 */
	sbios_move_to_dispatch(dd, rq);
}

/*
 * sbios_check_fifo returns 0 if there are no expired requests on the fifo,
 * 1 otherwise. Requires !list_empty(&dd->fifo_list[data_dir])
 */
static inline int sbios_check_fifo(struct sbios_data *dd, int ddir)
{
	struct request *rq = rq_entry_fifo(dd->fifo_list[ddir].next);

	/*
	 * rq is expired!
	 */
	if (time_after_eq(jiffies, (unsigned long)rq->fifo_time))
		return 1;

	return 0;
}

/*
 * For the specified data direction, return the next request to dispatch using
 * arrival ordered lists.
 */
static struct request *
sbios_fifo_request(struct sbios_data *dd, int data_dir)
{
	struct request *rq;

	if (WARN_ON_ONCE(data_dir != READ && data_dir != WRITE))
		return NULL;

	if (list_empty(&dd->fifo_list[data_dir]))
		return NULL;

	rq = rq_entry_fifo(dd->fifo_list[data_dir].next);
	if (data_dir == READ || !blk_queue_is_zoned(rq->q))
		return rq;

	/*
	 * Look for a write request that can be dispatched, that is one with
	 * an unlocked target zone.
	 */
	list_for_each_entry(rq, &dd->fifo_list[WRITE], queuelist) {
		if (blk_req_can_dispatch_to_zone(rq))
			return rq;
	}

	return NULL;
}

/*
 * For the specified data direction, return the next request to dispatch using
 * sector position sorted lists.
 */
static struct request *
sbios_next_request(struct sbios_data *dd, int data_dir)
{
	struct request *rq;

	if (WARN_ON_ONCE(data_dir != READ && data_dir != WRITE))
		return NULL;

	rq = dd->next_rq[data_dir];
	if (!rq)
		return NULL;

	if (data_dir == READ || !blk_queue_is_zoned(rq->q))
		return rq;

	/*
	 * Look for a write request that can be dispatched, that is one with
	 * an unlocked target zone.
	 */
	while (rq) {
		if (blk_req_can_dispatch_to_zone(rq))
			return rq;
		rq = sbios_latter_request(rq);
	}

	return NULL;
}

/*
 * sbios_dispatch_requests selects the best request according to
 * read/write expire, fifo_batch, etc
 */
static int sbios_dispatch_requests(struct request_queue *q, int force)
{
	struct sbios_data *dd = q->elevator->elevator_data;
	const int reads = !list_empty(&dd->fifo_list[READ]);
	const int writes = !list_empty(&dd->fifo_list[WRITE]);
	struct request *rq, *next_rq;
	int data_dir;

	/*
	 * batches are currently reads XOR writes
	 */
	rq = sbios_next_request(dd, WRITE);
	if (!rq)
		rq = sbios_next_request(dd, READ);

	if (rq && dd->batching < dd->fifo_batch)
		/* we have a next request are still entitled to batch */
		goto dispatch_request;

	/*
	 * at this point we are not running a batch. select the appropriate
	 * data direction (read / write)
	 */

	if (reads) {
		BUG_ON(RB_EMPTY_ROOT(&dd->sort_list[READ]));

		if (sbios_fifo_request(dd, WRITE) &&
		    (dd->starved++ >= dd->writes_starved))
			goto dispatch_writes;

		data_dir = READ;

		goto dispatch_find_request;
	}

	/*
	 * there are either no reads or writes have been starved
	 */

	if (writes) {
dispatch_writes:
		BUG_ON(RB_EMPTY_ROOT(&dd->sort_list[WRITE]));

		dd->starved = 0;

		data_dir = WRITE;

		goto dispatch_find_request;
	}

	return 0;

dispatch_find_request:
	/*
	 * we are not running a batch, find best request for selected data_dir
	 */
	next_rq = sbios_next_request(dd, data_dir);
	if (sbios_check_fifo(dd, data_dir) || !next_rq) {
		/*
		 * A sbios has expired, the last request was in the other
		 * direction, or we have run out of higher-sectored requests.
		 * Start again from the request with the earliest expiry time.
		 */
		rq = sbios_fifo_request(dd, data_dir);
	} else {
		/*
		 * The last req was the same dir and we have a next request in
		 * sort order. No expired requests so continue on from here.
		 */
		rq = next_rq;
	}

	/*
	 * For a zoned block device, if we only have writes queued and none of
	 * them can be dispatched, rq will be NULL.
	 */
	if (!rq)
		return 0;

	dd->batching = 0;

dispatch_request:
	/*
	 * rq is the selected appropriate request.
	 */
	dd->batching++;
	sbios_move_request(dd, rq);

	return 1;
}

/*
 * For zoned block devices, write unlock the target zone of completed
 * write requests.
 */
static void
sbios_completed_request(struct request_queue *q, struct request *rq)
{
	blk_req_zone_write_unlock(rq);
}

static void sbios_exit_queue(struct elevator_queue *e)
{
	struct sbios_data *dd = e->elevator_data;

	BUG_ON(!list_empty(&dd->fifo_list[READ]));
	BUG_ON(!list_empty(&dd->fifo_list[WRITE]));

	kfree(dd);
}

/*
 * initialize elevator private data (sbios_data).
 */
static int sbios_init_queue(struct request_queue *q, struct elevator_type *e)
{
	struct sbios_data *dd;
	struct elevator_queue *eq;

	eq = elevator_alloc(q, e);
	if (!eq)
		return -ENOMEM;

	dd = kzalloc_node(sizeof(*dd), GFP_KERNEL, q->node);
	if (!dd) {
		kobject_put(&eq->kobj);
		return -ENOMEM;
	}
	eq->elevator_data = dd;

	INIT_LIST_HEAD(&dd->fifo_list[READ]);
	INIT_LIST_HEAD(&dd->fifo_list[WRITE]);
	dd->sort_list[READ] = RB_ROOT;
	dd->sort_list[WRITE] = RB_ROOT;
	dd->fifo_expire[READ] = read_expire;
	dd->fifo_expire[WRITE] = write_expire;
	dd->writes_starved = writes_starved;
	dd->front_merges = 1;
	dd->fifo_batch = fifo_batch;

	spin_lock_irq(q->queue_lock);
	q->elevator = eq;
	spin_unlock_irq(q->queue_lock);
	return 0;
}

/*
 * sysfs parts below
 */

static ssize_t
sbios_var_show(int var, char *page)
{
	return sprintf(page, "%d\n", var);
}

static void
sbios_var_store(int *var, const char *page)
{
	char *p = (char *) page;

	*var = simple_strtol(p, &p, 10);
}

#define SHOW_FUNCTION(__FUNC, __VAR, __CONV)				\
static ssize_t __FUNC(struct elevator_queue *e, char *page)		\
{									\
	struct sbios_data *dd = e->elevator_data;			\
	int __data = __VAR;						\
	if (__CONV)							\
		__data = jiffies_to_msecs(__data);			\
	return sbios_var_show(__data, (page));			\
}
SHOW_FUNCTION(sbios_read_expire_show, dd->fifo_expire[READ], 1);
SHOW_FUNCTION(sbios_write_expire_show, dd->fifo_expire[WRITE], 1);
SHOW_FUNCTION(sbios_writes_starved_show, dd->writes_starved, 0);
SHOW_FUNCTION(sbios_front_merges_show, dd->front_merges, 0);
SHOW_FUNCTION(sbios_fifo_batch_show, dd->fifo_batch, 0);
#undef SHOW_FUNCTION

#define STORE_FUNCTION(__FUNC, __PTR, MIN, MAX, __CONV)			\
static ssize_t __FUNC(struct elevator_queue *e, const char *page, size_t count)	\
{									\
	struct sbios_data *dd = e->elevator_data;			\
	int __data;							\
	sbios_var_store(&__data, (page));				\
	if (__data < (MIN))						\
		__data = (MIN);						\
	else if (__data > (MAX))					\
		__data = (MAX);						\
	if (__CONV)							\
		*(__PTR) = msecs_to_jiffies(__data);			\
	else								\
		*(__PTR) = __data;					\
	return count;							\
}
STORE_FUNCTION(sbios_read_expire_store, &dd->fifo_expire[READ], 0, INT_MAX, 1);
STORE_FUNCTION(sbios_write_expire_store, &dd->fifo_expire[WRITE], 0, INT_MAX, 1);
STORE_FUNCTION(sbios_writes_starved_store, &dd->writes_starved, INT_MIN, INT_MAX, 0);
STORE_FUNCTION(sbios_front_merges_store, &dd->front_merges, 0, 1, 0);
STORE_FUNCTION(sbios_fifo_batch_store, &dd->fifo_batch, 0, INT_MAX, 0);
#undef STORE_FUNCTION

#define DD_ATTR(name) \
	__ATTR(name, 0644, sbios_##name##_show, sbios_##name##_store)

static struct elv_fs_entry sbios_attrs[] = {
	DD_ATTR(read_expire),
	DD_ATTR(write_expire),
	DD_ATTR(writes_starved),
	DD_ATTR(front_merges),
	DD_ATTR(fifo_batch),
	__ATTR_NULL
};

static struct elevator_type iosched_sbios = {
	.ops.sq = {
		.elevator_merge_fn = 		sbios_merge,
		.elevator_merged_fn =		sbios_merged_request,
		.elevator_merge_req_fn =	sbios_merged_requests,
		.elevator_dispatch_fn =		sbios_dispatch_requests,
		.elevator_completed_req_fn =	sbios_completed_request,
		.elevator_add_req_fn =		sbios_add_request,
		.elevator_former_req_fn =	elv_rb_former_request,
		.elevator_latter_req_fn =	elv_rb_latter_request,
		.elevator_init_fn =		sbios_init_queue,
		.elevator_exit_fn =		sbios_exit_queue,
	},

	.elevator_attrs = sbios_attrs,
	.elevator_name = "sbios",
	.elevator_owner = THIS_MODULE,
};

static int __init sbios_init(void)
{
	return elv_register(&iosched_sbios);
}

static void __exit sbios_exit(void)
{
	elv_unregister(&iosched_sbios);
}

module_init(sbios_init);
module_exit(sbios_exit);

MODULE_AUTHOR("Jens Axboe");
MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("sbios IO scheduler");
