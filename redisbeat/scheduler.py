# -*- coding: utf-8 -*-
# Copyright 2014 Kong Luoxing

# Licensed under the Apache License, Version 2.0 (the 'License'); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at http://www.apache.org/licenses/LICENSE-2.0
import datetime
from decimal import Decimal
import sys
import traceback
from time import mktime
from functools import partial

import jsonpickle
from celery.beat import Scheduler
from redis import StrictRedis
from redis.sentinel import Sentinel
from celery import current_app
from celery.utils.log import get_logger
from redis.exceptions import LockError
try:
    import urllib.parse as urlparse
except ImportError:
    from urllib.parse import urlparse
from cryptography.fernet import Fernet

logger = get_logger(__name__)
debug, linfo, error, warning = (logger.debug, logger.info, logger.error,
                                logger.warning)
try:
    MAXINT = sys.maxint
except AttributeError:
    # python3
    MAXINT = sys.maxsize
_PROTECTED_TYPES = (
    type(None),
    int,
    float,
    Decimal,
    datetime.datetime,
    datetime.date,
    datetime.time,
)


def is_protected_type(obj):
    """Determine if the object instance is of a protected type.

    Objects of protected types are preserved as-is when passed to
    force_str(strings_only=True).
    """
    return isinstance(obj, _PROTECTED_TYPES)
def force_bytes(s, encoding="utf-8", strings_only=False, errors="strict"):
    """
    Similar to smart_bytes, except that lazy instances are resolved to
    strings, rather than kept as lazy objects.

    If strings_only is True, don't convert (some) non-string-like objects.
    """
    # Handle the common case first for performance reasons.
    if isinstance(s, bytes):
        if encoding == "utf-8":
            return s
        else:
            return s.decode("utf-8", errors).encode(encoding, errors)
    if strings_only and is_protected_type(s):
        return s
    if isinstance(s, memoryview):
        return bytes(s)
    return str(s).encode(encoding, errors)

class RedisScheduler(Scheduler):
    fernet: Fernet | None = None

    def __init__(self, *args, **kwargs):
        app = kwargs['app']

        self.key = app.conf.get("redis_scheduler_url",
                                "celery:beat:order_tasks")
        self.schedule_url = app.conf.get("redis_scheduler_url",
                                         "redis://localhost:6379")
        # using sentinels
        # supports 'sentinel://:pass@host:port/db
        if self.schedule_url.startswith('sentinel://'):
            self.broker_transport_options = app.conf.broker_transport_options
            self.rdb = self.sentinel_connect(self.broker_transport_options.get("master_name", "mymaster"))
        else:
            self.rdb = StrictRedis.from_url(self.schedule_url)
        Scheduler.__init__(self, *args, **kwargs)
        app.add_task = partial(self.add, self)

        self.multi_node = app.conf.get("redis_multi_node_mode", False)
        # how long we should hold on to the redis lock in seconds
        if self.multi_node:
            self.lock_ttl = current_app.conf.get("redis_scheduler_lock_ttl", 30)
            self._lock_acquired = False
            self._lock = self.rdb.lock('celery:beat:task_lock', timeout=self.lock_ttl)
            self._lock_acquired = self._lock.acquire(blocking=False)

    def _remove_db(self):
        linfo("remove db now")
        self.rdb.delete(self.key)

    def _when(self, entry, next_time_to_run):
        return mktime(entry.schedule.now().timetuple()) + (self.adjust(next_time_to_run) or 0)

    def setup_schedule(self):
        # init entries
        self.merge_inplace(self.app.conf.beat_schedule)
        tasks = [jsonpickle.decode(self.fernet.decrypt(force_bytes(jsonpickle.decode(entry))) if self.fernet else entry) for entry in self.rdb.zrange(self.key, 0, -1)]
        linfo('Current schedule:\n' + '\n'.join(
              str('task: ' + entry.task + '; each: ' + repr(entry.schedule))
              for entry in tasks))

    def merge_inplace(self, tasks):
        old_entries = self.rdb.zrangebyscore(self.key, 0, MAXINT, withscores=True)
        old_entries_dict = dict({})
        for task, score in old_entries:
            if not task:
                break
            print("ready to load old_entries: %s", str(task))
            # Don't decrypt old_entries in the scheduler to prevent logging leaks.
            # We don't need it in this method anyway.
            encoded = self.fernet.decrypt(force_bytes(task)) if self.fernet else task
            entry = jsonpickle.decode(encoded)
            old_entries_dict[entry.name] = (entry, score)
        print("old_entries: %s", old_entries_dict)

        self.rdb.delete(self.key)

        for key in tasks:
            last_run_at = 0
            e = self.Entry(**dict(tasks[key], name=key, app=self.app))
            if key in old_entries_dict:
                # replace entry and remain old score
                last_run_at = old_entries_dict[key][1]
                del old_entries_dict[key]

            # Merge encrypted or encoded json entry into an ordered set of jobs
            encoded = jsonpickle.encode(e)
            json = self.fernet.encrypt(force_bytes(encoded)) if self.fernet else encoded
            self.rdb.zadd(self.key, {json: min(last_run_at, self._when(e, e.is_due()[1]) or 0)})
        debug("old_entries: %s",old_entries_dict)
        for key, tasks in old_entries_dict.items():
            debug("key: %s", key)
            debug("tasks: %s", tasks)
            debug("zadd: %s", self.rdb.zadd(self.key, {jsonpickle.encode(tasks[0]): tasks[1]}))
        debug(self.rdb.zrange(self.key, 0, -1))

    def is_due(self, entry):
        return entry.is_due()

    def adjust(self, n, drift=-0.010):
        if n and n > 0:
            return n + drift
        return n

    def add(self, **kwargs):
        e = self.Entry(app=current_app, **kwargs)
        encoded = force_bytes(jsonpickle.encode(e))
        json = self.fernet.encrypt(encoded) if self.fernet else encoded
        self.rdb.zadd(self.key, {json: self._when(e, e.is_due()[1]) or 0})
        return True

    def remove(self, task_key):
        tasks = self.rdb.zrange(self.key, 0, -1) or []
        for idx, task in enumerate(tasks):
            encoded = self.fernet.decrypt(force_bytes(task)) if self.fernet else task
            entry = encoded.decode()
            if entry.name == task_key:
                self.rdb.zremrangebyrank(self.key, idx, idx)
                return True
        else:
            return False

    def list(self):
        return [jsonpickle.decode(self.fernet.decrypt(force_bytes(entry)) if self.fernet else entry) for entry in self.rdb.zrange(self.key, 0, -1)]

    def get(self, task_key):
        tasks = self.rdb.zrange(self.key, 0, -1) or []
        for idx, task in enumerate(tasks):
            encoded = self.fernet.decrypt(force_bytes(task)) if self.fernet else task
            entry = jsonpickle.decode(encoded)
            if entry.name == task_key:
                return entry
        else:
            return None

    def tick(self):
        tasks = self.rdb.zrangebyscore(
            self.key, 0,
            self.adjust(mktime(self.app.now().timetuple()), drift=0.010),
            withscores=True) or []

        next_times = [self.max_interval, ]

        for task, score in tasks:
            entry = jsonpickle.decode(task)
            is_due, next_time_to_run = self.is_due(entry)

            next_times.append(next_time_to_run)
            if is_due:
                next_entry = self.reserve(entry)
                try:
                    linfo("add task entry: %s to publisher", entry.name)
                    result = self.apply_async(entry)
                except Exception as exc:
                    error('Message Error: %s\n%s',
                          exc, traceback.format_stack(), exc_info=True)
                else:
                    debug('%s sent. id->%s', entry.task, result.id)
                self.rdb.zrem(self.key, task)
                encoded = force_bytes(jsonpickle.encode(next_entry))
                self.rdb.zadd(self.key, {encoded: self._when(next_entry, next_time_to_run) or 0})

        next_task = self.rdb.zrangebyscore(self.key, 0, MAXINT, withscores=True, num=1, start=0)
        if not next_task:
            linfo("no next task found")
            return min(next_times)
        encoded = self.fernet.decrypt(force_bytes(next_task[0][0])) if self.fernet else next_task[0][0]
        entry = jsonpickle.decode(encoded)
        next_times.append(self.is_due(entry)[1])

        return min(next_times)

    def close(self):
        # it would be call after cycle end
        if self.multi_node:
            try:
                self._lock.release()
            except LockError:
                pass
        self.sync()

    def sentinel_connect(self, master_name):
        url = urlparse.urlparse(self.schedule_url)

        def parse_host(s):
            if ':' in s:
                host, port = s.split(':', 1)
                port = int(port)
            else:
                host = s
                port = 26379

            return host, port

        if '@' in url.netloc:
            auth, hostspec = url.netloc.split('@', 1)
        else:
            auth = None
            hostspec = url.netloc

        if auth and ':' in auth:
            _, password = auth.split(':', 1)
        else:
            password = None
        path = url.path
        if path.startswith('/'):
            path = path[1:]
        hosts = [parse_host(s) for s in hostspec.split(',')]
        sentinel = Sentinel(hosts, password=password, db=path)
        master = sentinel.master_for(master_name)
        return master

    @property
    def info(self):
        # return infomation about Schedule
        return '    . db -> {self.schedule_url}, key -> {self.key}'.format(self=self)
