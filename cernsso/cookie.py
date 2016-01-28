import os
import time
import sqlite3
import tempfile
import cookielib
import json
from tempfile import mkstemp
from urlparse import urlparse

from sh import cern_get_sso_cookie


DB_FILENAME = "cookie.db"
ROT_TIME = 24 * 60 * 60 # 24 hours


class CookieManager(object):
    """Obtain your CERN cookies here!"""
    def __init__(self, workdir):
        self.workdir = workdir
        self.certpath = os.path.join(workdir, "myCert.pem")
        self.keypath  = os.path.join(workdir, "myCert.key")

        self._ensure_db()

    def _ensure_db(self):
        dbpath = os.path.join(self.workdir, DB_FILENAME)
        self.conn = sqlite3.connect(dbpath)
        self.cursor = self.conn.cursor()

        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS cookies (
                domain text primary key,
                last_update unsigned big int,
                cookie text
            )
        """)
        self.conn.commit()

    def get_cookie(self, url):
        domain = urlparse(url).hostname
        self.cursor.execute("SELECT * FROM cookies WHERE domain=?", (domain, ) )
        res = self.cursor.fetchone()

        if not res:
            return self.get_new_cookie(url)

        domain, last_update, cookie = res

        if int(time.time()) - last_update > ROT_TIME:
            return self.get_new_cookie(url)

        return json.loads(cookie)

    def get_new_cookie(self, url, use_certs = True):
        _, cookietmp = mkstemp(prefix='.tmp', dir=self.workdir, text=True)
        params_dict = {"r": True,
                       "u": url,
                       "o": cookietmp}
        if use_certs:
            params_dict.update({"cert": self.certpath,
                                "key": self.keypath})

        cern_get_sso_cookie(**params_dict)
        cj = cookielib.MozillaCookieJar(cookietmp)
        cj.load()

        cookiedict = dict([(c.name, c.value) for c in cj])

        os.remove(cookietmp)

        self._save_cookie(url, json.dumps(cookiedict))

        return cookiedict

    def _save_cookie(self, url, cookie):
        self.cursor.execute(
            "INSERT OR REPLACE INTO cookies VALUES (?, ?, ?)",
            (urlparse(url).hostname, int(time.time()), cookie)
        )
        self.conn.commit()
