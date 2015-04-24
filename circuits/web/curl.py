# -*- coding: utf-8 -*-
# Modified version of curl_httpclient from python tornado
#
# Copyright 2009 Facebook
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""Non-blocking HTTP client implementation using pycurl."""

from __future__ import division, print_function, with_statement

import collections
import functools
import logging
import pycurl
import threading
import time

from circuits.core import BaseComponent, Event, handler, Timer
from circuits.core.utils import findcmp
from circuits.core.pollers import BasePoller, Poller

from circuits.web.client import request
from circuits.web.http import response
from circuits.web.client import parse_url
from circuits.web.headers import Headers
from circuits.web.wrappers import Request, Response
from circuits.web.parsers.http import HttpParser

from circuits.six import BytesIO, text_type, binary_type

logger = logging.getLogger(__name__)


class response_stream(Event):
    pass


class curl_timeout(Event):
    pass


class CurlHTTPClient(BaseComponent):

    channel = "client"

    def init(self, max_clients=10):
        self._poller = None
        self._multi = pycurl.CurlMulti()
        self._multi.setopt(pycurl.M_TIMERFUNCTION, self._set_timeout)
        self._multi.setopt(pycurl.M_SOCKETFUNCTION, self._handle_socket)
        self._curls = [self._curl_create() for i in range(max_clients)]
        self._free_list = self._curls[:]
        self._requests = collections.deque()
        self._fds = {}
        self._closed = False

        # libcurl has bugs that sometimes cause it to not report all
        # relevant file descriptors and timeouts to TIMERFUNCTION/
        # SOCKETFUNCTION.  Mitigate the effects of such bugs by
        # forcing a periodic scan of all active requests.
        self._force_timeout = Timer(10, curl_timeout(force=True),
                                    persist=True).register(self)
        self._timeout = Timer(360, curl_timeout(), persist=True).register(self)

        # Work around a bug in libcurl 7.29.0: Some fields in the curl
        # multi object are initialized lazily, and its destructor will
        # segfault if it is destroyed without having been used.  Add
        # and remove a dummy handle to make sure everything is
        # initialized.
        dummy_curl_handle = pycurl.Curl()
        self._multi.add_handle(dummy_curl_handle)
        self._multi.remove_handle(dummy_curl_handle)

    @handler("registered", "started", channel="*")
    def _on_registered_or_started(self, component, manager=None):
        if self._poller is None:
            if isinstance(component, BasePoller):
                self._poller = component
            else:
                if component is not self:
                    return
                component = findcmp(self.root, BasePoller)
                if component is not None:
                    self._poller = component
                else:
                    self._poller = Poller().register(self)

    def _set_timeout(self, msecs):
        """Called by libcurl to schedule a timeout."""
        self._timeout.reset(msecs / 1000.0)

    @handler("_read")
    def _handle_read(self, fd):
        #logger.debug("poller read wakeup %s", fd)
        action = pycurl.CSELECT_IN
        while True:
            try:
                ret, num_handles = self._multi.socket_action(fd, action)
            except pycurl.error as e:
                ret = e.args[0]
            if ret != pycurl.E_CALL_MULTI_PERFORM:
                break

    @handler("_write")
    def _handle_write(self, fd):
        action = pycurl.CSELECT_OUT
        while True:
            try:
                ret, num_handles = self._multi.socket_action(fd, action)
            except pycurl.error as e:
                ret = e.args[0]
            if ret != pycurl.E_CALL_MULTI_PERFORM:
                break

    @handler("curl_timeout")
    def _handle_timeout(self, force=False):
        while True:
            try:
                if force:
                    ret, num_handles = self._multi.socket_all()
                else:
                    ret, num_handles = self._multi.socket_action(
                        pycurl.SOCKET_TIMEOUT, 0)
            except pycurl.error as e:
                ret = e.args[0]
            if ret != pycurl.E_CALL_MULTI_PERFORM:
                break

        # In theory, we shouldn't have to do this because curl will
        # call _set_timeout whenever the timeout changes.  However,
        # sometimes after _handle_timeout we will need to reschedule
        # immediately even though nothing has changed from curl's
        # perspective.  This is because when socket_action is
        # called with SOCKET_TIMEOUT, libcurl decides internally which
        # timeouts need to be processed by using a monotonic clock
        # (where available) while tornado uses python's time.time()
        # to decide when timeouts have occurred.  When those clocks
        # disagree on elapsed time (as they will whenever there is an
        # NTP adjustment), tornado might call _handle_timeout before
        # libcurl is ready.  After each timeout, resync the scheduled
        # timeout with libcurl's current state.
        new_timeout = self._multi.timeout()
        if new_timeout >= 0:
            self._set_timeout(new_timeout)
        else:
            self._set_timeout(360 * 1000)

    def _process_queue(self):
        if self._poller is None:
            return

        started = 0
        while self._free_list and self._requests:
            started += 1
            curl = self._free_list.pop()
            req, kwargs = self._requests.popleft()
            res = Response(req)
            curl.info = {
                "parser": HttpParser(),
                "request": req,
                "response": res,
                "kwargs": kwargs,
                "curl_start_time": time.time()
            }
            self._curl_setup_request(curl)
            self._multi.add_handle(curl)
        self._handle_timeout()

    def _handle_socket(self, event, fd, multi, data):
        """Called by libcurl when it wants to change the file descriptors
        it cares about.
        """
        if event == pycurl.POLL_REMOVE:
            if fd in self._fds:
                self._poller.discard(fd)
                del self._fds[fd]
        else:
            # libcurl sometimes closes a socket and then opens a new
            # one using the same FD without giving us a POLL_NONE in
            # between.  This is a problem with the epoll IOLoop,
            # because the kernel can tell when a socket is closed and
            # removes it from the epoll automatically, causing future
            # update_handler calls to fail.  Since we can't tell when
            # this has happened, always use remove and re-add
            # instead of update.
            if fd in self._fds:
                self._poller.discard(fd)

            if event == pycurl.POLL_IN:
                self._poller.addReader(self, fd)
            elif event == pycurl.POLL_OUT:
                self._poller.addWriter(self, fd)
            elif event == pycurl.POLL_INOUT:
                self._poller.addReader(self, fd)
                self._poller.addWriter(self, fd)

            self._fds[fd] = event

    @handler("generate_events")
    def _on_generate_events(self):
        """Process any requests that were completed by the last
        call to multi.socket_action.
        """
        while not self._closed:
            num_q, ok_list, err_list = self._multi.info_read()
            for curl in ok_list:
                self._finish(curl)
            for curl, errnum, errmsg in err_list:
                self._finish(curl, errnum, errmsg)
            if num_q == 0:
                break

        if not self._closed:
            self._process_queue()

    @handler("close")
    def close(self):
        for curl in self._curls:
            curl.close()
        self._multi.close()
        self._timeout.unregister()
        self._force_timeout.unregister()
        self._closed = True

    @handler("request")
    def _on_request(self, method, url, body, headers, **kwargs):
        host, port, path, secure = parse_url(url)

        if not isinstance(headers, Headers):
            headers = Headers(headers.items())

        if "Host" not in headers:
            headers["Host"] = "{0:s}{1:s}".format(
                host, "" if port in (80, 443) else ":{0:d}".format(port)
            )

        req = Request(None,
                      method=method,
                      scheme="https" if secure else "http",
                      path=path,
                      headers=headers)
        req.url = url
        req.time = time.time()
        req.body = body
        self._requests.append((req, kwargs))
        self._process_queue()
        while True:
            res = yield self.wait("response")
            if not res.errors and res.value.request is not req:
                continue
            yield res
            break

    @handler("response")
    def _on_response(self, req, res):
        if req.stream_response:
            while any([c.info["request"] is req for c in self._curls
                       if c.info is not None]):
                yield
        if res._error is False:
            yield res
        else:
            raise RuntimeError("request failed with curl error")

    def _finish(self, curl, curl_error=None, curl_message=None):
        req = curl.info["request"]
        res = curl.info["response"]

        # the various curl timings are documented at
        # http://curl.haxx.se/libcurl/c/curl_easy_getinfo.html
        res.time_info = dict(
            queue=curl.info["curl_start_time"] - curl.info["request"].time,
            namelookup=curl.getinfo(pycurl.NAMELOOKUP_TIME),
            connect=curl.getinfo(pycurl.CONNECT_TIME),
            pretransfer=curl.getinfo(pycurl.PRETRANSFER_TIME),
            starttransfer=curl.getinfo(pycurl.STARTTRANSFER_TIME),
            total=curl.getinfo(pycurl.TOTAL_TIME),
            redirect=curl.getinfo(pycurl.REDIRECT_TIME),
        )

        if not req.stream_response:
            if curl_error:
                res.status = 599
                res.body = curl_message
                res.effective_url = None
                res.body = None
                self.fire(response(req, res))
            else:
                self.fire(self._prepare_response(curl))

        if curl_error:
            # hacky
            res._error = True
            logger.warning("request terminated with error (%s) %s",
                           curl_error, curl_message)
        else:
            res._error = False

        logger.debug("finishing %s", curl)
        curl.info = None
        self._multi.remove_handle(curl)
        self._free_list.append(curl)

    def _prepare_response(self, curl):
        parser = curl.info["parser"]
        req = curl.info["request"]
        res = curl.info["response"]

        res.headers = parser.get_headers()
        res.status = parser.get_status_code()
        res.effective_url = parser.get_url()
        if not req.stream_response:
            res._body.seek(0)

        return response(req, res)

    def _curl_create(self):
        curl = pycurl.Curl()
        curl.info = None
        if logger.isEnabledFor(logging.DEBUG):
            curl.setopt(pycurl.VERBOSE, 1)
            curl.setopt(pycurl.DEBUGFUNCTION, self._curl_debug)
        return curl

    def _curl_setup_request(self, curl):
        req = curl.info["request"]
        kwargs = curl.info["kwargs"]

        curl.setopt(pycurl.URL, binary_type(req.url))

        # libcurl's magic "Expect: 100-continue" behavior causes delays
        # with servers that don't support it (which include, among others,
        # Google's OpenID endpoint).  Additionally, this behavior has
        # a bug in conjunction with the curl_multi_socket_action API
        # (https://sourceforge.net/tracker/
        #  ?func=detail&atid=100976&aid=3039744&group_id=976),
        # which increases the delays.  It's more trouble than it's worth,
        # so just turn off the feature (yes, setting Expect: to an empty
        # value is the official way to disable this)
        if "Expect" not in req.headers:
            req.headers["Expect"] = ""

        # libcurl adds Pragma: no-cache by default; disable that too
        if "Pragma" not in req.headers:
            req.headers["Pragma"] = ""

        curl.setopt(pycurl.HTTPHEADER,
                    ["%s: %s" % (binary_type(k), binary_type(v))
                     for k, v in req.headers.items()])

        curl.setopt(pycurl.HEADERFUNCTION,
                    functools.partial(self._curl_header_callback, curl))

        response_body_file = kwargs.get("response_body_file")
        stream_response = kwargs.get("stream_response", False)
        req.stream_response = stream_response

        if stream_response and response_body_file is None:
            def write_function(component, curl, chunk):
                req = curl.info["request"]
                res = curl.info["response"]
                component.fire(response_stream(req, res, chunk))

            curl.setopt(pycurl.WRITEFUNCTION,
                        functools.partial(write_function, self, curl))
        else:
            res = curl.info["response"]
            res._body = response_body_file or BytesIO()
            curl.setopt(pycurl.WRITEFUNCTION, res._body.write)

        curl.setopt(pycurl.FOLLOWLOCATION,
                    kwargs.get("follow_redirects", True))
        curl.setopt(pycurl.MAXREDIRS, kwargs.get("max_redirects", 3))
        curl.setopt(pycurl.CONNECTTIMEOUT_MS,
                    int(1000 * kwargs.get("connect_timeout", 10)))
        if "request_timeout" in kwargs:
            curl.setopt(pycurl.TIMEOUT_MS,
                        int(1000 * kwargs.get("request_timeout", 10)))
        if "user_agent" in kwargs:
            curl.setopt(pycurl.USERAGENT,
                        binary_type(kwargs.get("user_agent")))
        else:
            curl.setopt(pycurl.USERAGENT, "Mozilla/5.0 (compatible; pycurl)")
        if "network_interface" in kwargs:
            curl.setopt(pycurl.INTERFACE, kwargs.get("network_interface"))
        if kwargs.get("decompress_response", False) is True:
            curl.setopt(pycurl.ENCODING, "gzip,deflate")
        else:
            curl.setopt(pycurl.ENCODING, "none")
        if "proxy_host" in kwargs and "proxy_port" in kwargs:
            curl.setopt(pycurl.PROXY, kwargs.get("proxy_host"))
            curl.setopt(pycurl.PROXYPORT, kwargs.get("proxy_port"))
            if "proxy_username" in kwargs:
                credentials = '%s:%s' % (kwargs.get("proxy_username"),
                                         kwargs.get("proxy_password", ""))
                curl.setopt(pycurl.PROXYUSERPWD, credentials)
        else:
            curl.setopt(pycurl.PROXY, '')
            curl.unsetopt(pycurl.PROXYUSERPWD)
        if kwargs.get("validate_cert", True) is True:
            curl.setopt(pycurl.SSL_VERIFYPEER, 1)
            curl.setopt(pycurl.SSL_VERIFYHOST, 2)
        else:
            curl.setopt(pycurl.SSL_VERIFYPEER, 0)
            curl.setopt(pycurl.SSL_VERIFYHOST, 0)
        if "ca_certs" in kwargs:
            curl.setopt(pycurl.CAINFO, kwargs.get("ca_certs"))
        else:
            # There is no way to restore pycurl.CAINFO to its default value
            # (Using unsetopt makes it reject all certificates).
            # I don't see any way to read the default value from python so it
            # can be restored later.  We'll have to just leave CAINFO untouched
            # if no ca_certs file was specified, and require that if any
            # request uses a custom ca_certs file, they all must.
            pass

        if kwargs.get("allow_ipv6", True) is False:
            # Curl behaves reasonably when DNS resolution gives an ipv6 address
            # that we can't reach, so allow ipv6 unless the user asks to
            # disable.
            curl.setopt(pycurl.IPRESOLVE, pycurl.IPRESOLVE_V4)
        else:
            curl.setopt(pycurl.IPRESOLVE, pycurl.IPRESOLVE_WHATEVER)

        # Set the request method through curl's irritating interface which
        # makes up names for almost every single method
        curl_options = {
            "GET": pycurl.HTTPGET,
            "POST": pycurl.POST,
            "PUT": pycurl.UPLOAD,
            "HEAD": pycurl.NOBODY,
        }
        custom_methods = set(["DELETE", "OPTIONS", "PATCH"])
        for o in curl_options.values():
            curl.setopt(o, False)
        if req.method in curl_options:
            curl.unsetopt(pycurl.CUSTOMREQUEST)
            curl.setopt(curl_options[req.method], True)
        elif kwargs.get("allow_nonstandard_methods", True) \
                or req.method in custom_methods:
            curl.setopt(pycurl.CUSTOMREQUEST, req.method)
        else:
            raise KeyError('unknown method ' + req.method)

        # Handle curl's cryptic options for every individual HTTP method
        if req.method == "GET":
            if req.body is not None:
                raise ValueError('Body must be None for GET req')
        elif req.method in ("POST", "PUT") or req.body is not None:
            if req.body is None:
                raise ValueError(
                    'Body must not be None for "%s" request' % req.method)

            request_buffer = BytesIO(text_type(req.body))

            def ioctl(cmd):
                if cmd == curl.IOCMD_RESTARTREAD:
                    request_buffer.seek(0)
            curl.setopt(pycurl.READFUNCTION, request_buffer.read)
            curl.setopt(pycurl.IOCTLFUNCTION, ioctl)
            if req.method == "POST":
                curl.setopt(pycurl.POSTFIELDSIZE, len(req.body))
            else:
                curl.setopt(pycurl.UPLOAD, True)
                curl.setopt(pycurl.INFILESIZE, len(req.body))

        if kwargs.get("auth_username") is not None:
            userpwd = "%s:%s" % (kwargs.get("auth_username"),
                                 kwargs.get("auth_password", ''))

            if kwargs.get("auth_mode") is None \
                    or kwargs.get("auth_mode") == "basic":
                curl.setopt(pycurl.HTTPAUTH, pycurl.HTTPAUTH_BASIC)
            elif kwargs.get("auth_mode") == "digest":
                curl.setopt(pycurl.HTTPAUTH, pycurl.HTTPAUTH_DIGEST)
            else:
                raise ValueError("Unsupported auth_mode %s"
                                 % kwargs.get("auth_mode"))

            curl.setopt(pycurl.USERPWD, binary_type(userpwd))
            logger.debug("%s %s (username: %r)", req.method, req.url,
                         kwargs.get("auth_username"))
        else:
            curl.unsetopt(pycurl.USERPWD)
            logger.debug("%s %s", req.method, req.url)

        if kwargs.get("client_cert") is not None:
            curl.setopt(pycurl.SSLCERT, kwargs.get("client_cert"))

        if kwargs.get("client_key") is not None:
            curl.setopt(pycurl.SSLKEY, kwargs.get("client_key"))

        if kwargs.get("ssl_options") is not None:
            raise ValueError("ssl_options not supported in curl http client")

        if threading.activeCount() > 1:
            # libcurl/pycurl is not thread-safe by default.  When multiple
            # threads are used, signals should be disabled.  This has the side
            # effect of disabling DNS timeouts in some environments (when
            # libcurl is not linked against ares), so we don't do it when there
            # is only one thread.  Applications that use many short-lived
            # threads may need to set NOSIGNAL manually in a
            # prepare_curl_callback since there may not be any other threads
            # running at the time we call threading.activeCount.
            curl.setopt(pycurl.NOSIGNAL, 1)

    def _curl_header_callback(self, curl, header_line):
        stream = curl.info["request"].stream_response
        parser = curl.info["parser"]
        parser.execute(header_line, len(header_line))
        if stream and parser.is_headers_complete():
            self.fire(self._prepare_response(curl))

    def _curl_debug(self, debug_type, debug_msg):
        debug_types = ('I', '<', '>', '<', '>')
        if debug_type == 0:
            logger.debug('%s', debug_msg.strip())
        elif debug_type in (1, 2):
            for line in debug_msg.splitlines():
                logger.debug('%s %s', debug_types[debug_type], line)
        elif debug_type == 4:
            logger.debug('%s %r', debug_types[debug_type], debug_msg)
