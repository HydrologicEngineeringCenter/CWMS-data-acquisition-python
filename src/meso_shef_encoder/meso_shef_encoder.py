#! /usr/bin/env python
'''____________________________________________________________________________
 - Created 3/31/2022 by Jim Terrell (Missouri Basin River Forecast Center) 
   ver 1.0
 - Notes: 
    - Should be Python 2 and 3 compatible
    - Assumes mesonet data is in metric units
    - Supported parameters: Depth-Snow(SD), Dir-Wind(UD), Irrad(RW), Precip(RW)
                            Speed-Wind(UP), Temp-Air(TE), %-SoilMoisture(MV)
                            Temp-Soil(TB)
    - Configurations could be split off into a file and imported if desired
    - Assumes precipitation and solar radiation are 5-minutes. Therefore, 
      hourly totals are calculated
    - Mesonet missing data flag was assumed to be "M". This can be changed 
      in the "Configurations" section below (missVal)
    - SHEF output files are grouped by state to limit excessive file sizes
      for the SHEF decoder
    - SHEF coding information: "Standard Hydrometeorological Exchange Format
      (SHEF) CODE Manual" July 5, 2012
        * For SHEF vector coding inforomation see p55, sect 7.4.6
        * Convert soil depths, windspeed, snow depth into English per 
          SHEF requirements ("MV & TB" - p55, sect 7.4.6; "US" could be mi/hr
          or m/s, it was easier to do mi/hr since english conversion was 
          already set up - Table 1, p1; "SD" could be IN or CM, it was easier 
          to do IN since english conversion was already set up; "TA" - p55, 
           sect 7.4.6)
        * For SHEF physical element definitions see Appendix G
        * For .E coding format see p6, Fig 2.3, ch 4, and ch 5
        * For information about metric vs. English unit coding see p33, 
          sect 7.1.1
____________________________________________________________________________'''

import os
import csv
import datetime
import decimal
import optparse
from dateutil import relativedelta, parser, tz

#************************  Configurations  ************************************
paramXref = {'%-RelativeHumidity': 'XR', # Cross referencece for USACE/SHEF ids
             'Depth-Snow': 'SD',  
             'Dir-Wind': 'UD', 
             'Irrad': 'RW',
             'Precip': 'PP', 
             'Speed-Wind': 'US',
             'Temp-Air': 'TA',
             '%-SoilMoisture': 'MV',
             'Temp-Soil': 'TB'}
valueMods = {'TB': ['vector', 'eng'] ,# Custom modifications to the values
             'MV': ['vector'],        # Make SHEF vector format
             'UD': ['R0'],            # Whole deg(R##, ## = number of decimals)
             'US': ['eng'],           # SHEF assumes MPH (doesn't convert)
             'SD': ['eng'],           # Accepts in or cm, english easier
             'TA': ['eng'],
             'PP': ['hrly', 'eng'],          # Sum up values to create hourly value
             'RW': ['hrly']}
p_describe = {'%-RelativeHumidity': 'Relative humidity (', # SHEF descripts
             'Depth-Snow': 'Snow depth (',
             'Dir-Wind': 'Wind direction (',
             'Irrad': 'Solar radiation (',
             'Precip': 'Precipitation (',
             'Speed-Wind': 'Wind speed (',
             'Temp-Air': 'Air temperature (',
             '%-SoilMoisture': 'Soil moisture (',
             'Temp-Soil': 'Soil temperature ('}
stateXref = {'M8': {'abrv': 'MT', 'name': 'Montana'}, # MBRFC LID/State 
             'W4': {'abrv': 'WY', 'name': 'Wyoming'},
             'C2': {'abrv': 'CO', 'name': 'Colorado'},
             'N8': {'abrv': 'ND', 'name': 'North Dakota'},
             'S2': {'abrv': 'SD', 'name': 'South Dakota'},
             'N1': {'abrv': 'NE', 'name': 'Nebraska'},
             'K1': {'abrv': 'KS', 'name': 'Kansas'},
             'M5': {'abrv': 'MN', 'name': 'Minnesota'},
             'I4': {'abrv': 'IA', 'name': 'Iowa'},
             'M7': {'abrv': 'MO', 'name': 'Missouri'}}
missVal = 'M'                           # Mesonet missing value indicator
lineLenMax = 65                         # Maximum length of SHEF line 
maxLocs = 60                            # Number of sites for SHEF message
tzs = {'C': tz.gettz('America/Chicago'), 
       'M': tz.gettz('America/Denver'),
       'UTC': tz.gettz('UTC')}
#******************************************************************************

# Process and check variables and arguments
class procVars():
    def __init__(self):
        ### Get system variables
        self.system_vars()
        
        ### Get user arguments 
        self.user_args()
    
    # System variables
    def system_vars(self):
        # Get current time and make timezone aware
        self.curTime = datetime.datetime.now(tz = tz.tzlocal())
        self.curTime = self.curTime.replace(tzinfo=tzs['UTC']) -       \
                                    self.curTime.utcoffset()
        
        # Set paths (script director and parent directory)
        self.sDir = os.path.dirname(os.path.realpath(__file__))
        self.pDir = os.path.dirname(self.sDir)
          
    # User Arguments - passed in from command line
    def user_args(self):
        # create optparse object
        p = optparse.OptionParser()

        # Set arguments that are required for running the program
        required="inFile".split(',')
        
        # List the various options user can specify and any defaults
        p.add_option("-i", '--inFile', dest='inFile', 
                     help='Data input file. Must be specific csv format. '
                     'Default: MesonetData.csv'
                     , default = '{}MesonetData.csv'
                     .format(os.path.join(self.sDir,'')))
        p.add_option("-o", '--outFile', dest='outFile', 
                     help='Output file basename. Note: State abbreviation and '
                     'current date will be added', default = '')
        p.add_option("-x", '--idXref', dest='idXref', 
                     help='ID cross reference file. Example: idXref.txt')
        dateTime = self.curTime.strftime('%Y%m%d_%H%M')
        
        # Get the options and values
        (self.options, args) = p.parse_args()
        self.options = vars(self.options)
        
        # Make sure required arguments are met
        for r in required:
            if self.options[r] is None:
                p.error("parameter %s required; \nfor list of parameters, "
                "type: meso_shef_encoder.py -h"%r )
                
        # Check each variable for validity
        for arg, value in sorted(self.options.items()):
           self.check_var(arg, value)
           
    # Function to check variables for certain requirements            
    def check_var(self, arg, value): 
        ### Check if file exists
        # Does it exist
        if arg == 'idXref':
            if value:
                if self.fileExists(value):
                    self.idXrefFname = value
                    # Load xRef file
                    self.idXref = self.getIdXref(value)
            else:
                self.idXref = ''
        if arg == 'inFile':
            if self.fileExists(value):
                self.inFile = value
        if arg == 'outFile':
            self.outFile = value
               
    # Check if a file exists - returns True if it does and exits if not
    def fileExists(self, value):
        if os.path.isfile(value):
            return True
        else:
            print('Error: The file "{}" does not exist.'.format(value))
            
    # Loads the id cross reference file into a dictionary
    def getIdXref(self, filename):
        xref = {}
        print('Loading id cross reference list from: {}'.format(filename))
        with open(filename, 'r') as f:
            data = f.read()
        for line in data.splitlines():
            if '#' not in line and not line.isspace():
                parsed = line.split('|')
                u_id = parsed[0].lstrip().rstrip()
                n_id = parsed[1].lstrip().rstrip()
                xref[u_id] = n_id
        return xref
    
# Functions to get, parse, filter, and process data                   
class dataTools():
    # Loads the input file containing the mesonet data into a list
    def getDataFile(self, inFile):
        print('Loading data from: {}'.format(inFile))
        with open(inFile, 'r') as f:
            csvReader = csv.reader(f)
            rawData = [row for row in csvReader]
        return rawData
    
    # Parses the mesonet data into a dictionary
    def parser(self, rawData):
        self.getHourly()
    
        # Initialize local variables
        data = {}   # Format: {usace_id:{sId:'foo',{usace_param:{date:value}}}}
        desc = ''
        self.idXref = procVars_inst.idXref
        hrly = {}   # Dictionary for params needing hourly sums
        for mP in self.getHourly()[1]:
            hrly[mP] = {'sum': 0, 'cnt': 0}
        
        # Iterate each row
        for row in rawData:
            # Skip empty rows
            if row:
                # Get id and soil depths
                if row[0] == 'B':
                    uId = self.getId(row)
                    dpths = [r for r in row]
                     
                    # Get sId 
                    sId = self.getShefId(uId)
                    
                    # Create a SHEF id if one could not be determined add 
                    # description to be used in SHEF message
                    desc = ''
                    if not sId:
                        sId = self.genShefId(uId)
                        desc = 'No valid lid was found for {}. Using: {}' \
                                .format(uId, sId)
                    
                    # Get state
                    st = self.getState(uId, sId)
                
                # Get columns
                if row[0] == 'C':
                    cols = [r for r in row]
                    
                    # Combine depths
                    cols = self.addDpths(dpths, cols)
                                    
                # Get units
                if row[0] == 'Units':
                    units = [r for r in row]
                    
                # Get data
                if row[0].isdigit():
                    # Continue if there is a valid sId
                    if sId:
                        date = row[cols.index('UTC')]
                        date = datetime.datetime.strptime(date, '%d%b%Y %H%M')
                        # Only keep top of the hour data
                        if date.minute == 00:
                            for i in range(len(cols)): 
                                c = cols[i]
                                sP = [sp for p,sp in paramXref.items() 
                                      if p in c]
                                u = units[i]
                                v = row[i]
                                # Replace value with hourly sum if 12 values
                                if c in hrly:
                                    if v:
                                        if self.isDigit(v):
                                            hrly[c]['sum'] += float(v)
                                            hrly[c]['cnt'] += 1
                                        # Only if 12 5-min values
                                        if hrly[c]['cnt'] == 12:
                                            v = str(hrly[c]['sum'])
                                        else:
                                            v = '-9999'
                                    else:
                                        v = '-9999'
                                        
                                    # Reset hourly accumulator
                                    hrly[c]['sum'] = 0
                                    hrly[c]['cnt'] = 0
                                
                                if i > 1 and c != 'UTC' and v:
                                    # Convert metric to English
                                    if sP[0] in valueMods:
                                        if 'eng' in valueMods[sP[0]]:
                                            v, u = self.toEnglish(v, u)
                                        elif 'metric' in valueMods[sP[0]]:
                                            v, u = self.toMetric(v, u)
                                    
                                    # Store in dictionary
                                    if len(v) > 0:
                                        if uId in data:
                                            if c in data[uId]:
                                                data[uId][c]['data'][date] = v
                                                if not data[uId][c]['units']:
                                                    data[uId][c]['units'] = u
                                            else:
                                                data[uId][c]={'units':u,
                                                             'data':{date:v}}
                                        else:
                                            data[uId] = {'sId':sId ,
                                                        'state': st,
                                                        'desc': desc,
                                                        c:{'units':u,
                                                           'data':{date: v}}}
                            
                        # Add data to hourly accumulator
                        else:
                            for i in range(len(cols)):
                                c = cols[i]
                                if c in hrly and self.isDigit(row[i]):
                                    v = float(row[i])
                                    hrly[c]['sum'] += v
                                    hrly[c]['cnt'] += 1
        return data        
    
    # Get mesonet location id from row - returns the id
    def getId(self, row):
        ######## for test
        return row[2]
        uId = {row[i].split('-')[0] for i in range(2, len(row))}
        if len(uId) == 1:
            return list(uId)[0]
    
    # Get combine depths with parameters - returns list of parameters
    def addDpths(self, dpths, cols):
        # extract depths from fields
        depths = ['_' + v.split('-D')[1] 
                  if '-D' in v else '' 
                  for v in dpths] 
        
        # combine depths with associated parameters
        params = [v for v in cols]
        params = [v + depths[i] for i,v in enumerate(params)]
        return params
    
    # Cross reference sId and/or check if it is valid
    def getShefId(self, uId):
        sId = None
        # Check length of uId
        if len(uId) == 5:
            # Check if last two characters are alphabetic and numeric
            if uId[3].isalpha() and uId[4].isdigit():
                # Assume is a valid sId
                return uId.upper()
                       
        # uId is not an sId, check cross reference list
        else:
            if self.idXref:
                sId = [n for u,n in self.idXref.items() if uId == u]
                if sId:
                    return sId[0].upper()
            
        # Use uId as SHEF id if 8 characters or less
        if len(uId) <= 8:
            print('WARNING: A NWSID could not be determined. Using {} since '
                  'it is 8 characters or less'.format(uId))
            return uId.upper()
        
        return None
    
    # Creates a SHEF id when one could not be determined from xref list
    def genShefId(self, uId):
        sId = uId[:8].upper()
        print('WARNING: {} is not a valid NWSLID and a cross reference ' \
                  'NWSLID was not found. Using {}'.format(uId, sId))
        return sId
                
    # Convert units to English
    def toEnglish(self, value, fromU):
        try:
            value = float(value)
            if value != -9999:
                # From millimiters to inches
                if fromU == 'mm':
                    return str((value/25.4)), 'in'
                # From kilometers per hour to miles per hour
                elif fromU == 'kph':
                    return str((value/1.609)), 'mph'
                # From Fahrenheit to Celcius
                elif fromU == 'C':
                    return str((value * (9/5)) + 32),'F'
                else:
                    print('WARNING: {} input unit is not supported for conversion'\
                           ' to English units'.format(fromU))
            else:
                return '-9999', ''
            
        except ValueError:
            print("WARNING: {} is not a valid number".format(value))
            return '-9999',''
        
        return str(value), fromU
        
    # Unit converter
    def toMetric(self, value, fromU):
        try:
            value = float(value)
            if value != -9999:
                # From in to mm
                if fromU == 'in':
                    return str(value * 25.4), 'mm'
                # From in to m
                elif fromU == 'in':
                    return str(value * 0.0254), 'm'
                # From F to C
                elif fromU == 'F':
                    return str(((value - 32) * 5)/9), 'C'
                # From langley/min to W/m^2
                elif fromU == 'langley/min':
                    return str(value * 697.3), 'W/m^2'
                else:
                    print('WARNING: {} input unit is not supported for '\
                          'conversion to metric units'.format(fromU))
            else:
                return '-9999',''
        except ValueError:
            print("WARNING: {} is not a valid number".format(value))
            return '-9999',''
        
        return str(value), fromU
        
    # Output data to file
    def toFile(self, outFile, data):
        print('Writing: {}'.format(outFile))
        with open(outFile, 'w') as f:
            f.write(data)
    
    # Create output filename if needed. Format: mesoID_SHEF_YYYYMMDD_HHMMSS.txt
    def outNameGen(self, outFile, st, i):
        # Generate filename if one was not specified by user
        dateTime = procVars_inst.curTime
        dateTime = dateTime.strftime('%Y%m%d_%H%M%S')
        if len(outFile) == 0:
            outFile = '{}_Mesonet_SHEF_{}_{}.txt'.format(st, dateTime, i)
        else:
            name = outFile.split('.')[0]
            ext = outFile.split('.')[1]
            outFile = '{}_{}_{}_{}.{}'.format(st, name, dateTime, i, ext)
        return outFile
    
    # Get state from uId or sId 
    def getState(self, uId, sId):
        if sId and len(sId) == 5:
            sfx = sId[3:]
            if sfx in stateXref:
                return stateXref[sfx]['abrv']
        prfx = uId[:2].upper()
        for k,v in stateXref.items():
            if prfx in v['abrv']:
                return prfx
        return 'MSG'
        
    # Get mesonet param names to do hourly calcs (output shef & meso list)
    def getHourly(self):
        hrlyShef = [k for k,v in valueMods.items() if 'hrly' in v]
        hrlyMeso = [k for p in hrlyShef for k,v in paramXref.items() if p in v]
        return hrlyShef, hrlyMeso
    
    # Check if string is a valid number
    def isDigit(self, v):
        try:
            float(v)
            return True
        except ValueError:
            return
                  
# SHEF encodes the data
class shefEncoder():
    # Generate SHEF messages
    def makeShef(self, data, st):
        messages = []
        # Create header
        header = self.header(st)
        
        # Create footer
        footer = self.footer()
        
        # Only create a message if there is a header
        if header:
            # Create bodies
            bodies = self.body(st, data)
            for body in bodies:
                message = header
                message += body
                message += footer
                messages.append(message)
            return messages
    
    # Create header of SHEF message
    def header(self, st):
        state = [v['name'] for k,v in stateXref.items() 
                if st in list(v.values())]
        if state:
            state = state[0]
            dateTime = procVars_inst.curTime.strftime('%d%H%M')
            header = '\nSRUS83 KKRF {}\n'.format(dateTime)
            header += 'RR8KRF\n\n'
            header += ': US Army Corp of Engineers\n'
            header += ': Upper Missouri River Basin Plains\n'
            header += ': Snow and Soil Moisture Network\n\n'
            header += ': {} Mesonet Data\n\n\n'.format(state)
            return header
        else:
            print('WARNING: {} is an inalid state, data skipped.'.format(st))
            return
        
    # Create the body of the SHEF message
    def body(self, st, data):
        bodies = []
        body = ''
        numLocs = 0
        sites = [s for s,v in data.items() if v['state'] == st]
        
        # Code to populate body
        for s in sites:
            numLocs += 1
            if numLocs > maxLocs:
                bodies.append(body)
                body = ''; numLocs = 0
            
            sId = data[s]['sId']
            params = [k for k in data[s].keys()]
        
            # Create section for each parameter
            for p in params:
                if p not in ['sId', 'state', 'desc']:
                    minDtTm = self.sDate(data[s][p]['data'])
                    if minDtTm:
                        minDt = minDtTm.strftime('%y%m%d')
                        tz = 'Z'
                        minTm = minDtTm.strftime('DH%H%M')
                        shef_p = self.getShefParam(p)
                        unit = data[s][p]['units']
                        if unit == '%':
                            unit = 'pct'
                        idDesc = data[s]['desc']
                        
                        if shef_p:
                            # Insert description of parameter and id if needed
                            # # # # # # # if idDesc:
                                # # # # # # # body+= ': {}\n'.format(idDesc)
                            pDesc = self.getDescript(shef_p, p, unit)
                            body += ': {}\n'.format(pDesc)
                            
                            # Create shef code
                            if shef_p in dataTools_inst.getHourly()[0]:
                                pedstep = '{}HRZ'.format(shef_p)
                            else:
                                pedstep = '{}IRZ'.format(shef_p)
                            body += '.E {:>}{:>7}{:>2}{:>7}'             \
                                       .format(sId, minDt, tz, minTm)
                            
                            # Declare metric or english units
                            if unit in ['in', 'ft', 'F', 'mph']:
                                body += '/DUE'
                            elif unit in ['%', 'pct', 'deg']:
                                body += ''
                            else:
                                body += '/DUS'                       
                            body += '/{}/DIH1\n'.format(pedstep)
                            
                            # Populate values
                            lLen = 1   
                            for dT in sorted(data[s][p]['data'].keys()):     
                                v = data[s][p]['data'][dT]
                                if len(v) > 0:  # Skip empty values
                                    # Start new line
                                    # Modify value if needed
                                    v = self.valueMod(v, shef_p, p)
                                    if lLen == 1:          
                                        body += '.E1 {}'.format(v)
                                        lLen += 4 + len(v)
                                    else:
                                        lLen += 2 + len(v)
                                        # Continue on same line til max len
                                        if lLen > 1 and lLen < lineLenMax:
                                            body += '/ {}'.format(v)
                                        # Start new line
                                        else:
                                            body += '\n.E1 {}'.format(v)
                                            lLen = 4 + len(v)
                        body += '\n\n'
        bodies.append(body)
        return bodies
        
    # Create footer of SHEF message
    def footer(self):
        footer = '\n:END OF MESSAGE\n'
        footer += 'NNNN'                     
        return footer
        
    # Find the minimum date 
    def sDate(self, data):
        if data:
            return min(data.keys())
    
    # Cross reference the usace parameter with SHEF parameter code
    def getShefParam(self, param):
        for k,v in paramXref.items():
            if k in param:
                return v
        
    # Adjust value or convert to vector value if necessary
    def valueMod(self, value, shef_p, usace_p):
        # 1. Assign and return missing value if missing
        if value == missVal:
            return '-9999'
        
        # 2. Check if value is a valid number
        try: 
            value = float(value)
        except ValueError:
            print("WARNING: {} is not a valid number".format(value))
            return
        
        # 3. Apply and return custom modifications
        if shef_p in valueMods:
            for mod in valueMods[shef_p]:
                # Make vector
                if 'vector' in mod:
                    vector = ''
                    depth = self.getDepth(usace_p)
                    
                    # Get value
                    value = float(value) / 1000
                    vector = '{:.4f}'.format(abs(value)).replace('0.','.')
                    vector = '{}{}'.format(depth, vector)
                    if value < 0:
                        vector = '-' + vector
                    return vector
                # Round value
                elif 'R' in mod:
                    return self.valRounder(value, mod)
                            
        # 4. Apply and return default mod (round to 2 decimals)
        return '{:>7.2f}'.format(value)
        
    # Get depth from usace parameter
    def getDepth(self, usace_p):
        # Get depth
        depth = ''.join(c for c in usace_p.split('_')[1] 
                        if c.isdigit())
        try:    
            depth = int(depth)
            return depth
        except ValueError:
            print("WARNING: {} is not a valid depth".format(depth))
            return
            
    # Get description of parameter from p_describe dict 
    def getDescript(self, shef_p, usace_p, unit):
        # Get description from configuration dictionary
        descript = p_describe[usace_p.split('_')[0]]
        
        # Add depth to vector parameters
        if shef_p in valueMods:
            if 'vector' in valueMods[shef_p]:
                depth = self.getDepth(usace_p)
                descript = '{}{} at {} inch depth)'.format(descript,unit,depth)
                return descript
        descript = '{}{})'.format(descript, unit)
        return descript    

    # Value rounder
    def valRounder(self, value, modCode):
        ctx = decimal.getcontext()
        ctx.rounding = decimal.ROUND_HALF_UP
        value = decimal.Decimal(value)
        p_val = int(modCode.replace('R', ''))
        return str(round(value, p_val))
       
   
#------------------------------------------------------------------------------ 
#------------------------------------------------------------------------------           
if __name__ == '__main__':
    # Create class instances
    procVars_inst = procVars()
    dataTools_inst = dataTools()
    shefEncoder_inst = shefEncoder()
    
    # Get raw data from input file
    inFile = procVars_inst.inFile
    rawData = dataTools_inst.getDataFile(inFile)
    
    # Parse data into dictionary
    data = dataTools_inst.parser(rawData)
    
    ### SHEF encode the data
    # Split into states and limit gages to maxLocs value 
    # due to possible size limitations
    states = list(set(v['state'] for k,v in data.items()))
    for st in states:
        messages = shefEncoder_inst.makeShef(data, st)
        
        # Create SHEF files
        for i,m in enumerate(messages):
            outFile = dataTools_inst.outNameGen(procVars_inst.outFile, st, i)
            if m:
                dataTools_inst.toFile(outFile, m)
    
