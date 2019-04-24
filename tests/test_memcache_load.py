# -*- coding: utf-8 -*-
import os
import gzip
import logging

from memcache_load.memc_load import main, parse_appsinstalled
from pylibmc import Client

TESTDIR = os.path.dirname(os.path.abspath(__file__))


class Options(object):
    dry = False
    log = None
    pattern = os.path.join(TESTDIR, 'data', 'sample.tsv.gz')
    test = False
    threads = 1
    idfa = '127.0.0.1:11211'
    gaid = '127.0.0.1:11211'
    adid = '127.0.0.1:11211'
    dvid = '127.0.0.1:11211'


def test_memcache_load(caplog):
    caplog.set_level(logging.INFO)
    # Загрузим данные в memcache
    main(Options)
    # переименуем обратно тесовый файл
    modifed = os.path.join(TESTDIR, 'data', '.sample.tsv.gz')
    os.rename(modifed, Options.pattern)
    lastlog = caplog.records.pop().message
    assert "Acceptable error rate (0.0). Successfull load" in lastlog

    # Проверим загрузились ли наши данные
    client = Client(['127.0.0.1:11211'])
    with gzip.open(Options.pattern) as fd:
        for line in fd:
            app = parse_appsinstalled(line)
            key = "%s:%s" % (app.dev_type, app.dev_id)
            value = repr(client.get(key))
            assert value is not None
            #  TODO compare unicode values
