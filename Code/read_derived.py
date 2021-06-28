# -*- coding: utf-8 -*-

__all__ = ['derived','ascii_to_dataframe']

def igra(ident, filename, variables=None, levels=None, return_table=False, **kwargs):
    """ Read derived data for IGRA station
    Args:
        ident (str): IGRA ID
        filename (str): filename to read from
        variables (list): select only these variables
        levels (list): interpolate to these pressure levels [Pa]
        return_table (bool): return odb like datatable
        **kwargs:
    Returns:
        Dataset : derived information 
        Dataset : station information
    """
    import xarray as xr
    if '.nc' in filename:
        data = xr.open_dataset(filename, **kwargs)
    else:
        data, station = to_std_levels(ident, filename, levels=levels, return_table=return_table, **kwargs)

    if variables is not None:
        avail = list(data.data_vars.keys())
        if not isinstance(variables, list):
            variables = [variables]

        variables = [iv for iv in variables if iv in avail]
        if len(variables) > 0:
            data = data[variables]  # subset

    return data, station



def ascii_to_dataframe(filename, get_levels=False, **kwargs):
    """Read IGRA version 2 Data from NOAA
    Args:
        filename (str): Filename
        get_levels(bool): return data for level information --> False only returns header with derived parameters 
    Returns:
        DataFrame : Table of radiosonde soundings with date as index and variables as columns
        DataFrame : Station Information
    Info:
        Format Description of IGRA 2 Sounding Data Files
    ---------------------
    Notes:
    ---------------------

    Format of the IGRA-Derived Sounding Parameters Files

    ---------------------
    Notes:
    ---------------------

    1. Sounding-derived parameters are available for a subset of the soundings 
    in IGRA. This subset includes soundings at fixed observing stations on 
    land that contain temperature observations and a surface pressure level.
    The parameters include precipitable water between the surface and 500 hPa,
    the refractive index, vertical gradients of several variables, and 
    various measures of boundary-layer characteristics and stability.

    2. The derived parameters are updated once a day in the early morning Eastern
    Time.

    3. Each "-drvd.txt.zip" file contains the sounding-derived parameters for one 
    station. The name of the file corresponds to a station's IGRA 2 identifier
    (e.g., "USM00072201-drvd.txt.zip"  contains the derived parameters for the 
    station with the identifier USM00072201).

    4. Each file contains a series of multiline sounding records, each of which 
    consists of a header record followed by one line of data for each pressure 
    level in the sounding. Non-pressure levels are not included.

    5. All missing parameter values are coded as -99999. 

    ---------------------
    Header Record Format:
    ---------------------

    ---------------------------------
    Variable   Columns  Type
    ---------------------------------
    HEADREC       1-  1  Character
    ID            2- 12  Character
    YEAR         14- 17])
    MONTH        19- 20])
    DAY          22- 23])
    HOUR         25- 26])
    RELTIME      28- 31])
    NUMLEV       32- 36])
    PW           38- 43])
    INVPRESS     44- 49])
    INVHGT       50- 55])
    INVTEMPDIF   56- 61])
    MIXPRESS     62- 67])
    MIXHGT       68- 73])
    FRZPRESS     74- 79])
    FRZHGT       80- 85])
    LCLPRESS     86- 91])
    LCLHGT       92- 97])
    LFCPRESS     98-103])
    LFCHGT      104-109])
    LNBPRESS    110-115])
    LNBHGT      116-121])
    LI          122-127])
    SI          128-133])
    KI          134-139])
    TTI         140-145])
    CAPE        146-151])
    CIN         152-157])
    ---------------------------------

    These variables have the following definitions:

    HEADREC		is the header record indicator (always set to "#").

    ID		is the station identification code. See "igra2-stations.txt"
            for a complete list of stations and their names and locations.

    YEAR 		is the year of the sounding.

    MONTH 		is the month of the sounding.

    DAY 		is the day of the sounding.

    HOUR 		is the hour of the sounding (99 = missing).

    RELTIME 	is the release time of the sounding (format HHMM, missing=9999).

    NUMLEV 		is the number of levels in the sounding (i.e., the number of 
            data records that follow).

    PW 		is the precipitable water (mm*100) between the surface and 500 hPa.

    INVPRESS 	is the pressure (in Pa or mb*100) at the level of the
            warmest temperature in the sounding. Only provided if
            the warmest temperature is above the surface.

    INVHGT 		is the height (in meters above the surface) of the warmest
            temperature in the sounding. Only provided when the
            warmest temperature is above the surface.

    INVTEMPDIF 	is the difference between the warmest temperature in the
            sounding and the surface temperature (K * 10). Only provided if
            the warmest temperature is above the surface.

    MIXPRESS 	is the pressure (in Pa or mb * 100) at the top of the
            mixed layer as determined using the parcel method.

    MIXHGT 		is the height (in meters above the surface) of the top of the
            mixed layer As determined using the parcel method.

    FRZPRESS 	is the pressure (in Pa or mb * 100) where the temperature
            first reaches the freezing point when moving upward from
            the surface. Determined by interpolating linearly with respect
            to the logarithm of pressure between adjacent reported levels.
            Not provided if the surface temperature is below freezing.

    FRZHGT 		is the height (in meters above the surface) where the temperature
            first reaches the freezing point when moving upward from the
            surface. Determined analogously to FRZPRESS. Not provided if the 
            surface temperature is below freezing.

    LCLPRESS 	is the pressure (in Pa or mb * 100) of the lifting condensation
            level.

    LCLHGT 		is the height (in meters above the surface) of the lifting
            condensation level.

    LFCPRESS 	is the pressure (in Pa or mb * 100) of the level of free convection.

    LFCHGT 		is the height (in meters above the surface) of the level of free
            convection.

    LNBPRESS 	is the pressure (in Pa or mb * 100) of the level of
            neutral buoyancy (or equilibrium level).

    LNBHGT 		is the height (in meters above the surface) of the level of
            neutral buoyancy (or equilibrium level).

    LI 		is the lifted index (in degrees C).

    SI 		is the Showalter index (in degrees C).

    KI 		is the K index (in degrees C).

    TTI 		is the total totals index (in degrees C).

    CAPE 		is the convective available potential energy (in J/kg).

    CIN 		is the convective inhibition (in J/kg).


    ---------------------
    Data Record Format:
    ---------------------

    In the data records following each header record, the first record always 
    represents the surface level. The variables in each data record include the 
    following:

    -------------------------------
    Variable        Columns Type  
    -------------------------------
    PRESS           1-  7 ])
    REPGPH          9- 15 ])
    CALCGPH        17- 23 ])
    TEMP           25- 31 ])
    TEMPGRAD       33- 39 ])
    PTEMP          41- 47 ])
    PTEMPGRAD      49- 55 ])
    VTEMP          57- 63 ])
    VPTEMP         65- 71 ])
    VAPPRESS       73- 79 ])
    SATVAP         81- 87 ])
    REPRH          89- 95 ])
    CALCRH         97-103 ])
    RHGRAD        105-111 ])
    UWND          113-119 ])
    UWDGRAD       121-127 ])
    VWND          129-135 ])
    VWNDGRAD      137-143 ])
    N             145-151 ])
    -------------------------------

    These variables have the following definitions:

    PRESS 		is the reported pressure (Pa or mb * 100).

    REPGPH 		is the reported geopotential height (meters). This value is
            often not available at significant levels.
            
    CALCGPH 	is the calculated geopotential height (meters). The geopotential
            height has been estimated by applying the hydrostatic balance to
            the atmospheric layer between the next lower level with a 
            reported geopotential height and the current level.
            
    TEMP 		is the reported temperature (K * 10).

    TEMPGRAD 	is the temperature gradient between the current level and
            the next higher level with a temperature [(K/km) * 10, positive
            if temperature increases with height].
            
    PTEMP 		is the potential temperature (K * 10).

    PTEMPGRAD 	is the potential temperature gradient between the current level
            and the next higher level with a potential temperature 
            [(K/km) * 10, positive if potential temperature increases 
            with height].

    VTEMP 		is the virtual temperature (K * 10).

    VPTEMP 		is the virtual potential temperature (K * 10).

    VAPPRESS 	is the vapor pressure (mb * 1000) as computed from temperature,
            pressure, and dewpoint depression at the same level.
            
    SATVAP 		is the saturation vapor pressure (mb * 1000) as computed from 
            pressure and temperature at the same level.
                    
    REPRH 		is the relative humidity (Percent * 10) as reported in the
            original sounding.

    CALCRH		is the relative humidity (Percent * 10) as calculated from vapor
            pressure, saturation vapor pressure, and pressure at the same 
            level.
            
    RHGRAD 		is the relative humidity gradient between the current level and
            the next higher usable level [(%/km) * 10, positive if relative 
            humidity increases with height].
            
    UWND 		is the zonal wind component [(m/s) * 10] as computed from the
            reported wind speed and direction.
            
    UWDGRAD 	is the vertical gradient of the zonal wind between the current
            level and the next higher level with a wind observation 
            [(m/s per km) * 10, positive if zonal wind becomes more 
            positive with height].
            
    VWND 		is the meridional wind component [(m/s) * 10] as computed 
            from the reported wind speed and direction.
            
    VWNDGRAD 	is the vertical gradient of the meridional wind component 
            between the current level and the next higher level with a wind
            observation [(m/s per km) * 10, positive if the meridional 
            wind becomes more positive with height].

    N 		is the refractive index (unitless).

    --------------------------------------------------------------------------------
    --------------------------------------------------------------------------------
    """

    import datetime
    import gzip
    import zipfile
    import os
    import io
    import numpy as np
    import pandas as pd
    from . import support as sp

    if not os.path.isfile(filename):
        raise IOError("File not Found! %s" % filename)

    if '.zip' in filename:
        archive = zipfile.ZipFile(filename, 'r')
        inside = archive.namelist()
        tmp = archive.open(inside[0])
        tmp = io.TextIOWrapper(tmp, encoding='utf-8')
        tmp = tmp.read()
        archive.close()
        data = tmp.splitlines()  # Memory (faster)
    elif '.gz' in filename:
        with gzip.open(filename, 'rt', encoding='utf-8') as infile:
            tmp = infile.read()  # alternative readlines (slower)
            data = tmp.splitlines()  # Memory (faster)
    else:
        with open(filename, 'rt') as infile:
            tmp = infile.read()  # alternative readlines (slower)
            data = tmp.splitlines()  # Memory (faster)

    raw = []
    headers = []
    dates = []
    for i, line in enumerate(data):
        if line[0] == '#':
            ident = line[1:12]
            year = line[13:17]
            month = line[18:20]
            day = line[21:23]
            hour = line[24:26]
            reltime = line[27:31]
            numlev = int(line[32:36])
            pw = int(line[37:45])
            invpress = int(line[43:49])
            invhgt  = int(line[49:55])
            invtempdif = int(line[55:61])
            mixpress = int(line[61:67])
            mixhgt = int(line[67:73])
            frzpress = int(line[73:79])
            frzhgt = int(line[79:85])
            lclpress = int(line[85:91])
            lclhgt = int(line[91:97])
            lfcpress = int(line[97:103])
            lfchgt = int(line[103:109])
            lnbp = int(line[109:115])
            lnbhgt = int(line[115:121])
            li = int(line[121:127])
            si = int(line[127:133])
            ki = int(line[133:139])
            tti  = int(line[139:145])
            cape = int(line[145:151])
            cin = int(line[151:157])


            if int(hour) == 99:
                time = reltime + '00'
            else:
                time = hour + '0000'

            # wired stuff !?
            if '99' in time:
                time = time.replace('99', '00')

            idate = datetime.datetime.strptime(year + month + day + time, '%Y%m%d%H%M%S')
            headers.append((idate, numlev, reltime, numlev, pw, invpress, invhgt, invtempdif, 
                            mixpress, mixhgt, frzpress, frzhgt, lclpress, lclhgt, lfcpress, 
                            fchgt, lnbp, lnbhgt, li, si, ki, tti, cape, cin))

        else:
            # Data
            press = int(line[0:7])          #PRESS           1-  7   Integer
            repgph = int(line[8:15])        #REPGPH          9- 15   Integer
            calcgph = int(line[16:23])      #CALCGPH        17- 23   Integer
            temp = int(line[24:31])         #TEMP           25- 31   Integer
            tempgrad = int(line[32:39])     #TEMPGRAD       33- 39   Integer
            ptemp = int(line[40:47])        #PTEMP          41- 47   Integer
            ptempgrad = int(line[48:55])    #PTEMPGRAD      49- 55   Integer
            vtemp = int(line[56:63])        #VTEMP          57- 63   Integer
            vptemp = int(line[64:71])       #VPTEMP         65- 71   Integer
            vappress = int(line[72:79])     #VAPPRESS       73- 79   Integer
            satvp = int(line[80:87])        #SATVAP         81- 87   Integer
            reprh = int(line[88:95])        #REPRH          89- 95   Integer
            calcrh = int(line[96:103])       #CALCRH         97-103   Integer
            rhgrad = int(line[104:111])       #RHGRAD        105-111   Integer
            uwnd = int(line[112:119])         #UWND          113-119   Integer
            uwndgrad = int(line[120:127])     #UWDGRAD       121-127   Integer
            vwnd = int(line[128:135])         #VWND          129-135   Integer
            vwndgrad = int(line[136:143])     #VWNDGRAD      137-143   Integer
            n = int(line[145:151])            #N             145-151   Integer

            raw.append((press, repgph, calcgph, temp, tempgrad, ptemp, ptempgrad, vtemp,
                        vptemp, vappress, satvp, reprh, calcrh, rhgrad, uwnd, uwndgrad,
                        vwnd, vwndgrad, n))

            dates.append(idate)

    c = ['press', 'repgph', 'calcgph', 'temp', 'tempgrad', 'ptemp', 'ptempgrad', 'vtemp',
                        'vptemp', 'vappress', 'satvp', 'reprh', 'calcrh', 'rhgrad', 'uwnd', 'uwndgrad',
                        'vwnd', 'vwndgrad', 'n']

    out = pd.DataFrame(data=raw, index=dates, columns=c)
    out = out.replace([-999.9, -9999, -8888, -888.8], np.nan)  # known missing values by IGRAv2
    out.index.name = 'date'

    headers = pd.DataFrame(data=headers, columns=['idate', 'numlev', 'reltime', 'numlev', 'pw', 'invpress', 'invhgt', 'invtempdif', 
                                                'mixpress', 'mixhgt', 'frzpress', 'frzhgt', 'lclpress', 'lclhgt', 'lfcpress', 
                                                'fchgt', 'lnbp', 'lnbhgt', 'li', 'si', 'ki', 'tti', 'cape', 'cin']).set_index('date') 

    
    return out, headers 