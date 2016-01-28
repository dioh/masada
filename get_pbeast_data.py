from cernsso import cookie
import requests
cm = cookie.CookieManager("/tmp") # Sqlite3 db with cookie will be saved there.
cookies = cm.get_new_cookie("https://atlasop.cern.ch/operation.php", use_certs=False)

def get(*args, **kwargs):

    stime = kwargs['stime']
    etime = kwargs['etime']
    partition = kwargs['partition']
    typ3 = kwargs['typ3']
    server = kwargs['server']
    attrib = kwargs['attrib']
    object_regxp = kwargs['object_regxp']
    
    id_str = ".".join([partition, typ3, attrib, server, object_regxp])

    parameters = {
        "id": id_str,
        "maxDataPoints": "5000",
        "plotType": "line",
        "to": etime,
        "from": stime,
    }
    response = requests.get("https://atlasop.cern.ch/tdaq/pbeast/readSeries",
                            cookies = cookies, verify=False, params = parameters)

    return response.text


