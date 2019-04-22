#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
import collections

from Queue import Queue
from threading import Thread
from optparse import OptionParser
import appsinstalled_pb2
import memcache

NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


class Worker(Thread):
    def __init__(self, queue, options):
        Thread.__init__(self)
        self.queue = queue
        self.daemon = True
        self.device_memc = {
                "idfa": options.idfa,
                "gaid": options.gaid,
                "adid": options.adid,
                "dvid": options.dvid,
            }
        self.device_memc_clients = dict()
        for name, addr in self.device_memc.items():
            self.device_memc_clients[name] = memcache.Client([addr])

        self.start()

    def processing(self, filename):
        processed = errors = 0
        logging.info('Processing %s' % filename)
        fd = gzip.open(filename)
        for line in fd:
            line = line.strip()
            if not line:
                continue
            appsinstalled = parse_appsinstalled(line)
            if not appsinstalled:
                errors += 1
                continue
            # memc_addr = self.device_memc.get(appsinstalled.dev_type)
            memc_client = self.device_memc_clients.get(appsinstalled.dev_type, None)

            if not memc_client:
                errors += 1
                logging.error("Unknow device type: %s" % appsinstalled.dev_type)
                continue
            # ok = insert_appsinstalled(memc_addr, appsinstalled, options.dry)
            ok = insert_appsinstalled(memc_client, appsinstalled, False)
            if ok:
                processed += 1
            else:
                errors += 1
        if not processed:
            fd.close()
            dot_rename(self.filename)
            # continue

        err_rate = float(errors) / processed
        if err_rate < NORMAL_ERR_RATE:
            logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
        else:
            logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))
        fd.close()
        dot_rename(filename)

    def run(self):
        while True:
            try:
                job = self.queue.get()
                if isinstance(job, str) and job == 'quit':
                    break
                self.processing(job)
            finally:
                self.queue.task_done()


class ThreadPool(object):
    """
    Класс организующий распределение заданий между потоками
    """

    def __init__(self, num_threads, config):
        self.num_threads = num_threads
        self.queue = Queue(self.num_threads)
        for _ in range(num_threads):
            Worker(self.queue, config)

    def add_task(self, task):
        self.queue.put(task)

    def wait_completion(self):
        for _ in xrange(self.num_threads):
            self.add_task('quit')

        self.queue.join()


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def insert_appsinstalled(memc_client, appsinstalled, dry_run=False):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    # @TODO persistent connection
    # @TODO retry and timeouts!
    try:
        if dry_run:
            logging.debug("%s - %s -> %s" % (memc_client, key, str(ua).replace("\n", " ")))
        else:
            memc_client.set(key, packed)
    except Exception, e:
        logging.exception("Cannot write to memc %s: %s" % (memc_client, e))
        return False
    return True


def parse_appsinstalled(line):
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def main(options):
    pool = ThreadPool(4, options)
    for fn in glob.iglob(options.pattern):
        pool.add_task(fn)
    pool.wait_completion()


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception, e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
