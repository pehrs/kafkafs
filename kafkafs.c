/*
 * Copyright (c) 2014 by Matti Pehrs
 * Author: matti@pehrs.com
 *
 * KafkaFS - Kafka FUSE File System
 * Simple Fuse module to send messages to Kafka via fuse.
 *
 */
#define FUSE_USE_VERSION 28

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <signal.h>
#include <stdio.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <sys/time.h>
#include <syslog.h>
#include <unistd.h>

#include <librdkafka/rdkafka.h>

struct kafkafs_config {
  char *brokers;
  int dbg;
};
struct kafkafs_config kfs_conf;

/*===================== */
/* syslog stuff */
/*===================== */
#define dolog(prio, fmt, ...) if(kfs_conf.dbg || prio == LOG_ERR) {openlog ("kafka-fuse-module", LOG_CONS | LOG_PID | LOG_NDELAY, LOG_LOCAL1); syslog(prio, fmt, ##__VA_ARGS__); closelog();}


/*===================== */
/* Kafka stuff */
/*===================== */
static rd_kafka_t *rk;
static rd_kafka_topic_conf_t *topic_conf;
static time_t kafka_start = -1L;
static long kafka_msg_counter = 0;


/**
 * Kafka Message delivery report callback.
 */
static void kafka_msg_delivered (rd_kafka_t *rk,
                                 void *payload, 
                                 size_t len,
                                 rd_kafka_resp_err_t error_code,
                                 void *opaque, 
                                 void *msg_opaque) {
  // Ignore for now...
	if (error_code) {
    dolog(LOG_ERR, "%% Message delivery failed: %s\n",
          rd_kafka_err2str(error_code));
  } else {
    dolog(LOG_INFO, "%% Message delivered (%zd bytes)\n", len);
  }

}

/*
 * FIXME: This is not called right now, why?
 */
static void kafka_logger (const rd_kafka_t *rk, int level,
		    const char *fac, const char *buf) {
	struct timeval tv;
	gettimeofday(&tv, NULL);
  dolog(LOG_INFO, "%u.%03u RDKAFKA-%i-%s: %s: %s\n",
        (int)tv.tv_sec, (int)(tv.tv_usec / 1000),
        level, fac, rd_kafka_name(rk), buf);
}



/**
 * Init Kafka Connection
 */
void kafka_init() {
	rd_kafka_conf_t *conf;
	char errstr[512];

  dolog(LOG_INFO, "kafka_init()\n");

	/* Kafka configuration */
	conf = rd_kafka_conf_new();

	/* Topic configuration */
  topic_conf = rd_kafka_topic_conf_new();

  rd_kafka_conf_set_dr_cb(conf, kafka_msg_delivered);

  /* Create Kafka handle */
  if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf,
                          errstr, sizeof(errstr)))) {
    dolog(LOG_ERR, "%% Failed to create new Kafka producer: %s\n",
           errstr);
    exit(1);
  }

  /* Set logger */
  rd_kafka_set_logger(rk, kafka_logger);
  rd_kafka_set_log_level(rk, LOG_DEBUG);
  
  /* Add brokers */
  if (rd_kafka_brokers_add(rk, kfs_conf.brokers) == 0) {
    dolog(LOG_ERR, "%% No valid kafka brokers specified\n");
    exit(1);
  }
  

  /* Get the start time */
  kafka_start = time(0);

}

static rd_kafka_topic_t *rkt = NULL;

void kafka_send_msg(const char *topic, const char* payload) {

  dolog(LOG_INFO, "kafka-send-msg %s (partition %d)\n", payload, RD_KAFKA_PARTITION_UA);

  /* Create topic */
  if(rkt == NULL || strcmp(rd_kafka_topic_name(rkt), topic) != 0) {
    if(rkt!=NULL) {
      /* Destroy old */
      rd_kafka_topic_destroy(rkt);
    }
    /* Create new topic */
    rkt = rd_kafka_topic_new(rk, topic, topic_conf);
  }

  dolog(LOG_INFO, "topic->%s\n", rd_kafka_topic_name(rkt));

  if (rd_kafka_produce(rkt, 
                       RD_KAFKA_PARTITION_UA,
                       RD_KAFKA_MSG_F_COPY,
                       /* Payload and length */
                       (char*)payload, strlen(payload) - 1,
                       /* Optional key and its length */
                       NULL, 0,
                       /* Message opaque, provided in
                        * delivery report callback as
                        * msg_opaque. */
                       NULL) == -1) {
    dolog(LOG_ERR, "%% Failed to produce to topic %s partition %i: %s\n",
           rd_kafka_topic_name(rkt), 
           RD_KAFKA_PARTITION_UA,
           rd_kafka_err2str(rd_kafka_errno2err(errno)));

    /* Poll to handle delivery reports */
    rd_kafka_poll(rk, 0);
  }
  /* Poll to handle delivery reports */
  rd_kafka_poll(rk, 0);
  
  kafka_msg_counter++;  
}

/*===================== */
/* Fuse stuff */
/*===================== */
static const char *kfs_topic_path = "/topic";
static const char *kfs_status_path = "/kafka-status";

static void* kfs_fuse_init(struct fuse_conn_info *conn) {

  dolog(LOG_INFO, "kfs_fuse_init()\n");

  // Init kafka
  kafka_init();

  return NULL;
}


static int kfs_open(const char *path, struct fuse_file_info *fi)
{

  dolog(LOG_INFO, "OPEN PATH: %s\n", path);

  fi->nonseekable=1;

  // Allow anything
	return 0;
}


static int kfs_write(const char *path, 
                     const char *buf, 
                     size_t size,
                     off_t offset, 
                     struct fuse_file_info *fi)
{
  char topic[256];

  strcpy(topic, path + 1 + strlen(kfs_topic_path));

  dolog(LOG_INFO, "WRITE: %s topic=%s, size=%ld, offset=%ld\n", path, topic, size, offset);

  // FIXME: We should break up the buf into lines separated by newlines and send them one-by-one

  // FIXME: What about rate-limiting stuff here?

  // Assume buf is a null terminated string...
  kafka_send_msg(topic, buf);

  // Allways succeed for now...
	return size;
}


static int kfs_getattr(const char *path, struct stat *stbuf)
{
  dolog(LOG_INFO, "GET ATTR: %s\n", path);

  // Anything goes
	int res = 0;

	memset(stbuf, 0, sizeof(struct stat));
	if (strcmp(path, "/") == 0) {    
    // The root
		stbuf->st_mode = S_IFDIR | 0755;
		stbuf->st_nlink = 1;
  } else if (strcmp(path, kfs_status_path) == 0) {
    // kafka-status path
		stbuf->st_mode = S_IFREG | 0444;
		stbuf->st_nlink = 1;
		stbuf->st_size = 2048;
	} else if (strcmp(path, kfs_topic_path) == 0) {
    // The topics
		stbuf->st_mode = S_IFDIR | 0755;
		stbuf->st_nlink = 1;
		stbuf->st_size = 0; // Fake size
	} else {
    // Anything else is a file :-)
		stbuf->st_mode = S_IFREG | 0664;
		stbuf->st_nlink = 1;
		stbuf->st_size = 0; // Fake size...
  }

	return res;
}

static int kfs_truncate(const char *path, off_t size)
{
  dolog(LOG_INFO, "truncate: %s\n", path);

  // Nothing needed for now...

	return 0;
}

static int kfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
			 off_t offset, struct fuse_file_info *fi)
{

  dolog(LOG_INFO, "READ DIR: %s\n", path);

	if (path == NULL)
		return -ENOENT;

  // Only list the root path
	if (strcmp(path, "/") != 0)
		return -ENOENT;

	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);
	filler(buf, kfs_status_path + 1, NULL, 0);
	filler(buf, kfs_topic_path + 1, NULL, 0);

	return 0;
}

static int kfs_read(const char *path, char *buf, size_t size, off_t offset,
		      struct fuse_file_info *fi)
{
	size_t len;
	(void) fi;
  char kfs_str[2048];

  time_t now = time(0);
    
  time_t diff = now - kafka_start;

  double msgs_per_sec = (double)kafka_msg_counter / (double)diff;

  sprintf(kfs_str, "Nof msgs: %ld\nMsgs/sec: %f\n", kafka_msg_counter, msgs_per_sec);

  dolog(LOG_INFO, "READ: %s\n", path);

  // Only allow to read the /kafka-status file
	if(strcmp(path, kfs_status_path) != 0)
		return -ENOENT;

	len = strlen(kfs_str);
	if (offset < len) {
		if (offset + size > len)
			size = len - offset;
		memcpy(buf, kfs_str + offset, size);
	} else
		size = 0;

	return size;
}

static int kfs_mknod(const char *path, mode_t mode, dev_t rdev)
{

  dolog(LOG_INFO, "MKNOD: %s\n", path);

  // Just fake for now...
	return 0;
}

static int kfs_release(const char *path, struct fuse_file_info *fi)
{
	/* Just a stub.	 This method is optional and can safely be left
	   unimplemented */

	(void) path;
	(void) fi;
  dolog(LOG_INFO, "RELEASE: %s\n", path);
	return 0;
}



static struct fuse_operations kfs_oper = {
	.getattr	= kfs_getattr,
	.mknod		= kfs_mknod,
	.truncate	= kfs_truncate,
	.open		= kfs_open,
	.read		= kfs_read,
	.write	= kfs_write,
	.release	= kfs_release,
	.readdir	= kfs_readdir,
  .init = kfs_fuse_init,
};

void usage(const char *pgm) {
  fprintf(stderr, "Usage: %s [-v] [-b brokers] file-system-path\n",
          pgm);
  fprintf(stderr, "      -h   Show this help page\n");
  fprintf(stderr, "      -v   Verbose output (to syslog)\n");
  fprintf(stderr, "      -b   Specify the list of brokers (and ports)\n");
  exit(EXIT_FAILURE);
}


enum {
     KEY_HELP,
     KEY_VERSION,
};

#define KAFKAFS_OPT(t, p, v) { t, offsetof(struct kafkafs_config, p), v }

static struct fuse_opt kafkafs_opts[] = {
     KAFKAFS_OPT("brokers=%s",       brokers, 0),
     KAFKAFS_OPT("debug",            dbg, 1),
     KAFKAFS_OPT("nodebug",          dbg, 0),
     KAFKAFS_OPT("--debug=true",     dbg, 1),
     KAFKAFS_OPT("--debug=false",    dbg, 0),

     FUSE_OPT_KEY("-V",             KEY_VERSION),
     FUSE_OPT_KEY("--version",      KEY_VERSION),
     FUSE_OPT_KEY("-h",             KEY_HELP),
     FUSE_OPT_KEY("--help",         KEY_HELP),
     FUSE_OPT_END
};

static int kafkafs_opt_proc(void *data, 
                            const char *arg, 
                            int key, 
                            struct fuse_args *outargs)
{
     switch (key) {
     case KEY_HELP:
       fprintf(stderr,
               "usage: %s mountpoint [options]\n"
               "\n"
               "general options:\n"
               "    -o opt,[opt...]  mount options\n"
               "    -h   --help      print help\n"
               "    -V   --version   print version\n"
               "\n"
               "KafkaFS options:\n"
               "    -o brokers=list-of-brokers-and-ports\n"
               "    -o debug\n"
               "    -o nodebug\n"
               "    --debug=BOOL    same as 'debug' or 'nodebug'\n"
               "\n"
               , outargs->argv[0]);
             fuse_opt_add_arg(outargs, "-ho");
             fuse_main(outargs->argc, outargs->argv, &kfs_oper, NULL);
             exit(1);

     case KEY_VERSION:
             fprintf(stderr, "KafkaFS version %s\n", PACKAGE_VERSION);
             fuse_opt_add_arg(outargs, "--version");
             fuse_main(outargs->argc, outargs->argv, &kfs_oper, NULL);
             exit(0);
     }
     return 1;
}

int main(int argc, char *argv[]) {

  struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
  
  memset(&kfs_conf, 0, sizeof(kfs_conf));
  
  fuse_opt_parse(&args, &kfs_conf, kafkafs_opts, kafkafs_opt_proc);
  
  return fuse_main(args.argc, args.argv, &kfs_oper, NULL);
}
