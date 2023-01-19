

def save_new_cache_folder():
    ticker_paths = glob.glob('cache/*')
    d = collections.defaultdict(list)
    import shutil

    import datetime as dt 
    import collections
    for t in ticker_paths:
        d[t.replace('cache/', '').split('-')[0]].append(t)

    def todt(path):
        if path.endswith('.csv'):
            if '2012' in path:
                return dt.datetime.strptime(path.split('.')[-2][-10:], '%Y-%m-%d').date()
        return  dt.datetime.strptime('1970-08-09', '%Y-%m-%d').date()

    dd = {}
    for k, v in d.items():
        if ds:
            dd[k] = max(v, key=todt)

    for ticker, path in dd.items():
        shutil.copyfile(path, 'newcache/' + ticker + '.csv')
