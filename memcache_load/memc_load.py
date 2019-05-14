#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
import collections
import time

from Queue import Queue
from threading import Thread
from optparse import OptionParser
import appsinstalled_pb2
from memcache import Client

AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])

NORMAL_ERR_RATE = 0.01
TIMEOUT = 5
MAXATTEMPTS = 5


class Worker(Thread):
    def __init__(self, queue, dry, device_memc, memc_clients):
        Thread.__init__(self)
        self.queue = queue
        self.daemon = True
        self.dry = dry
        self.device_memc = device_memc
        self.device_memc_clients = memc_clients

        self.start()

    def insert_appsinstalled(self, memc_client, appsinstalled, dry_run=False):
        ua = appsinstalled_pb2.UserApps()
        ua.lat = appsinstalled.lat
        ua.lon = appsinstalled.lon
        key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
        ua.apps.extend(appsinstalled.apps)
        packed = ua.SerializeToString()
        try:
            if dry_run:
                logging.debug("%s - %s -> %s" % (memc_client, key, str(ua).replace("\n", " ")))
                return True
            else:
                return self.memc_set(memc_client, key, packed)
        except Exception, e:
            logging.exception("Cannot write to memc %s: %s" % (memc_client, e))
            return False

    def memc_set(self, client, key, value):
        res = None
        for _ in range(MAXATTEMPTS):
            try:
                res = client.set(key, value)
            except Exception as error:
                dev_type = key.split(':')[0]
                self.device_memc_clients[dev_type] = Client(client.addresses)
            if res:
                return res
            time.sleep(TIMEOUT)
        # Remove broken server from dict
        self.device_memc_clients.pop(dev_type)
        raise Exception('Max attempts exceeded while trying to connect to {} with error {}'.format(
            self.device_memc[dev_type]), error)

    def processing(self, filename, dry=False):
        processed = errors = 0
        logging.info('Processing %s' % filename)

        with gzip.open(filename) as fd:
            for line in fd:
                line = line.strip()
                if not line:
                    continue
                appsinstalled = parse_appsinstalled(line)
                if not appsinstalled:
                    errors += 1
                    continue
                memc_client = self.device_memc_clients.get(appsinstalled.dev_type, None)

                if not memc_client:
                    errors += 1
                    logging.error("Unknow device type: %s" % appsinstalled.dev_type)
                    continue
                ok = self.insert_appsinstalled(memc_client, appsinstalled, dry)
                if ok:
                    processed += 1
                else:
                    errors += 1
            try:
                err_rate = float(errors) / processed
            except ZeroDivisionError:
                err_rate = float('inf')

            if err_rate < NORMAL_ERR_RATE:
                logging.debug("processed %s" % processed)
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
                self.processing(job, self.dry)
            finally:
                self.queue.task_done()


class ThreadPool(object):
    """
    Класс организующий распределение заданий между потоками
    """

    def __init__(self, num_threads, options):
        self.num_threads = num_threads
        self.queue = Queue(self.num_threads)

        self.device_memc = {
                "idfa": options.idfa,
                "gaid": options.gaid,
                "adid": options.adid,
                "dvid": options.dvid,
            }

        self.device_memc_clients = dict()

        for name, addr in self.device_memc.items():
            self.device_memc_clients[name] = Client([addr])

        for _ in range(num_threads):
            Worker(self.queue, options.dry, self.device_memc, self.device_memc_clients)

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
    max_threads = int(options.threads)
    pool = ThreadPool(max_threads, options)
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
    op.add_option("--pattern", action="store", default="memcache_load/data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    op.add_option("--threads", action="store", default=4)
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
