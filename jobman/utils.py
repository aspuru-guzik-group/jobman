def get_key_or_attr(src=None, key=None, default=...):
    try: return src[key]
    except: pass
    try: return getattr(src, key)
    except: pass
    if default is not ...: return default
    raise KeyError(key)
