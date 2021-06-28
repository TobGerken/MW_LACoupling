#!/usr/bin/env python
# coding: utf-8

# In[ ]:


__all__ = ['derived']

def derived(ident, directory, server=None, verbose=1):
    """ Download IGRAv2 Dervided from NOAA
    Args:
        ident (str): IGRA ID
        directory (str): output directory
        server (str): download url
        verbose (int): verboseness
    """
    import urllib
    import os
    from .support import message
    os.makedirs(directory, exist_ok=True)
    if server is None:
        server = 'https://www1.ncdc.noaa.gov/pub/data/derived/derived-por'
    url = "%s/%s-data.txt.zip" % (server, ident)
    message(url, ' to ', directory + '/%s-drvd.txt.zip' % ident, verbose=verbose)

    urllib.request.urlretrieve(url, directory + '/%s-drvd.txt.zip' % ident)

    if os.path.isfile(directory + '/%s-data.txt.zip' % ident):
        message("Downloaded: ", directory + '/%s-drvd.txt.zip' % ident, verbose=verbose)
    else:
        message("File not found: ", directory + '/%s-drvd.txt.zip' % ident, verbose=verbose)

