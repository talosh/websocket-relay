#!/usr/bin/env python
import logging
import tornado.escape
import tornado.ioloop
import tornado.options
import tornado.web
import tornado.websocket
import os.path

from tornado.options import define, options

define("port", default=8888, help="run on the given port", type=int)
define("secrets", multiple=True, help="upstream secret tokens", type=str)

secret_to_url = {}

class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/", MainHandler),
            (r"/upload/(.*)", StreamHandler),
            (r"/live/(.*\.ts)", SocketHandler),
        ]
        settings = dict(
            template_path=os.path.join(os.path.dirname(__file__), 'templates'),
            static_path=os.path.join(os.path.dirname(__file__), 'static'),
            max_body_size = 1024 * 1024 * 1024,
        )
        super(Application, self).__init__(handlers, **settings)


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.render('view-stream.html')

'''
@tornado.web.stream_request_body
class StreamHandler(tornado.web.RequestHandler):
    def data_received(self, data):
        secret = self.request.path.split('/')[-1]
        if secret not in secret_to_url:
            logging.info('Failed Stream Connection: %s - wrong secret.', self.request.remote_ip)
            self.write_error(403)
            return
        url = secret_to_url[secret]
        # logging.info('Data received for secret: %s, broadcasting to: %s', secret, url)
        SocketHandler.broadcast(data, url)
        # logging.info('Broadcasted data to WebSocket for URL: %s', url)
'''

@tornado.web.stream_request_body
class StreamHandler(tornado.web.RequestHandler):
    def data_received(self, data):
        try:
            secret = self.request.path.split('/')[-1]
            if secret not in secret_to_url:
                logging.info('Failed Stream Connection: %s - wrong secret.', self.request.remote_ip)
                self.write_error(403)
                return
            url = secret_to_url[secret]
            # logging.info('Data received for secret: %s, broadcasting to: %s', secret, url)
            SocketHandler.broadcast(data, url)
            # logging.info('Broadcasted data to WebSocket for URL: %s', url)
        except Exception as e:
            logging.error("Error in data_received: %s", str(e))
            self.write_error(500)

    def write_error(self, status_code, **kwargs):
        if status_code == 500:
            logging.error("Internal server error while processing request")
        elif status_code == 403:
            self.set_status(403)
            self.finish("Forbidden: wrong secret")
        else:
            super(StreamHandler, self).write_error(status_code, **kwargs)

'''
class SocketHandler(tornado.websocket.WebSocketHandler):
    waiters = {}

    def check_origin(self, origin):
        return True

    def open(self, url):
        url = f'live/{url}'
        logging.info('WebSocket open for URL: %s', url)
        if url not in SocketHandler.waiters:
            SocketHandler.waiters[url] = set()
        SocketHandler.waiters[url].add(self)
        logging.info('New WebSocket Connection for %s: %d total', url, len(SocketHandler.waiters[url]))

    def select_subprotocol(self, subprotocol):
        if len(subprotocol):
            return subprotocol[0]
        return super().select_subprotocol(subprotocol)

    def on_message(self, message):
        pass

    def on_close(self):
        for waiters in SocketHandler.waiters.values():
            if self in waiters:
                waiters.remove(self)
                logging.info('Disconnected WebSocket: %d total', len(waiters))

    @classmethod
    def broadcast(cls, data, url):
        if url in cls.waiters:
            # logging.info('Broadcasting data to %d waiters for URL: %s', len(cls.waiters[url]), url)
            for waiter in cls.waiters[url]:
                try:
                    waiter.write_message(data, binary=True)
                    # logging.info('Data successfully sent to a waiter for URL: %s', url)
                except tornado.websocket.WebSocketClosedError:
                    logging.error("Error sending message", exc_info=True)
'''

class SocketHandler(tornado.websocket.WebSocketHandler):
    waiters = {}

    def check_origin(self, origin):
        return True

    def open(self, url):
        url = f'live/{url}'
        logging.info('WebSocket open for URL: %s', url)
        if url not in SocketHandler.waiters:
            SocketHandler.waiters[url] = set()
        SocketHandler.waiters[url].add(self)
        logging.info('New WebSocket Connection for %s: %d total', url, len(SocketHandler.waiters[url]))

    def on_message(self, message):
        pass

    def on_close(self):
        for waiters in SocketHandler.waiters.values():
            if self in waiters:
                waiters.remove(self)
                logging.info('Disconnected WebSocket: %d total', len(waiters))

    @classmethod
    def broadcast(cls, data, url):
        if url in cls.waiters:
            # logging.info('Broadcasting data to %d waiters for URL: %s', len(cls.waiters[url]), url)
            for waiter in cls.waiters[url]:
                try:
                    waiter.write_message(data, binary=True)
                    # logging.info('Data successfully sent to a waiter for URL: %s', url)
                except tornado.websocket.WebSocketClosedError:
                    logging.error("Error sending message", exc_info=True)
        else:
            pass
            # logging.info('No waiters to broadcast to for URL: %s', url)


def main():
    tornado.options.parse_command_line()

    if options.secrets:
        for secret in options.secrets:
            # Associate each secret with a corresponding URL
            secret_to_url[secret] = f'live/{secret}.ts'
            logging.info('Mapping secret %s to URL %s', secret, secret_to_url[secret])

    secret_to_url['english'] = f'live/english.ts'
    secret_to_url['bulgarian'] = f'live/bulgarian.ts'
    secret_to_url['czech'] = f'live/czech.ts'
    secret_to_url['french'] = f'live/french.ts'
    secret_to_url['german'] = f'live/german.ts'
    secret_to_url['hungarian'] = f'live/hungarian.ts'
    secret_to_url['italian'] = f'live/italian.ts'
    secret_to_url['polish'] = f'live/polish.ts'
    secret_to_url['portuguese'] = f'live/portuguese.ts'
    secret_to_url['russian'] = f'live/russian.ts'
    secret_to_url['spanish'] = f'live/spanish.ts'
    secret_to_url['ukrainian'] = f'live/ukrainian.ts'
    
    app = Application()
    app.listen(options.port)
    logging.info('Application started on port %d', options.port)
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()
