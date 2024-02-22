#!/wm/lrl/localsoft/python3/bin/python3
import CDAPost
import datetime
import requests

def Parse_E(block,shef_crit):
    path = ''
    values = []
    loc = block.split()[1]
    date = block.split()[2]
    TZ = NWSTZ2CDATZ(block.split()[3])
    hectime = TimeString2UnixTime(date + block.split()[4].split('/')[0][2:],'%Y%m%d%H%M',TZ)
    if block.find('/DC'):
        i = 2
    else:
        i = 1
    code = block.split('/')[i][:2]
    if len(block.split('/')[i]) >= 5:
        version = block.split('/')[i][-4:-1]
    else:
        version = 'RZZ'
    interval = block.split('/')[i+1]
    print (version)
    print (interval)
    print (block)
    if interval[2] == 'H':
        factor = 60*60*1000
    elif interval[2] == 'M':
        factor = 60*1000
    elif interval[2] == 'D':
        factor = 24*60*60*1000
    time_increment = int(interval[3:].split()[0]) * factor
    chunks = block.strip().split('/')
    while chunks[0][:2] != 'DI':
        chunks = chunks[1:]
    chunks[0] = chunks[0].split()[-1]
    for chunk in chunks:
        if chunk != '':
            values.append([hectime,float(chunk),0])
            hectime += time_increment
    if loc in shef_crit.keys():
        if code in shef_crit[loc].keys():
            if version in shef_crit[loc][code].keys():
                units = shef_crit[loc][code][version]['Units']
                path = shef_crit[loc][code][version]['Path']
                return path,values,units
    return '','',''
def Parse_A(block,shef_crit):
    ts_array = []
    loc = block.split()[1]
    if loc in shef_crit.keys():
        date = block.split()[2]
        t = block.split('/')[0][-4:]
        data_blocks = block.split('/')[1:]
        TZ = NWSTZ2CDATZ(block.split()[3])
        hectime = TimeString2UnixTime(date + t,'%Y%m%d%H%M',TZ)
        for data_block in data_blocks:
            code,value = data_block.split()
        if loc in shef_crit.keys():
            if code in shef_crit[loc].keys():
                if len(code) > 2:
                    code,version = code[:2],code[2:]
                else:
                    version = 'RZZ'
                if version in shef_crit[loc][code].keys():
                    units = shef_crit[loc][code][version]['Units']
                    path = shef_crit[loc][code][version]['Path']
                    values = [[hectime,value,0]]
                    ts_array.append([path,values,units])
    return ts_array
def Parse_B(block,shef_crit):
    TS_Array = []
    return TS_Array
def load_shef_crit(filename):
    f = open(filename,'r')
    lines = f.readlines()
    shef_crit = {}
    for line in lines:
        if line[0] not in  ['#',' ','\n']:
            line = line[:-1]
            loc,pe,version,q = line.split('=')[0].split('.')
            path = line.split('=')[1].split(';')[0]
            if loc not in shef_crit.keys():
                shef_crit[loc] = {}
            if pe not in shef_crit[loc].keys():
                shef_crit[loc][pe] = {}
            if version not in shef_crit[loc][pe].keys():
                shef_crit[loc][pe][version] = {}
            shef_crit[loc][pe][version]['Path'] = path
            for i in range(1,len(line.split(';'))):
                shef_crit[loc][pe][version][line.split(';')[i].split('=')[0]] = line.split(';')[i].split('=')[1]
    return shef_crit

def NWSTZ2CDATZ(TZ):
    if TZ == 'Z':
        TZ = 'UTC'
    if len(TZ) == 2:
        TZ = TZ+'T'
    return TZ
def TimeString2UnixTime(TimeString,Format,Timezone='UTC'):
    epoch = datetime.datetime(1970, 1, 1, 0, 0)
    t = datetime.datetime.strptime(TimeString,Format) 
    try:
        tz = timezone(Timezone)
        t_with_tz = tz.localize(t)
        hectime = t_with_tz*1000
    except:
        TZs = {'CST':6,'CDT':5,'EST':5,'EDT':4,'UTC':0}
        if Timezone in TZs:
            hectime = (int(t.timestamp() - epoch.timestamp()) + TZs[Timezone]*60*60) * 1000  
            print (hectime,t)
    return hectime

def UnixTime2TimeString(Milliseconds,Format,Timezone='UTC'):
    t = datetime.datetime.fromtimestamp(Milliseconds/1000)
    tz = timezone(Timezone)
    t = t.astimezone(tz)
    return t.strftime(Format)


def process_shef(shef_file,crit_file):
    data = {}
    shef_crit = load_shef_crit(crit_file)
    f = open(shef_file,'r')
    lines = f.readlines()
    blocks = []
    block = ''
    for line in lines:
        line = line.strip('\n')
        if line.split(' ')[0] in ['.E','.ER','.A','.AR','.B']:
            if block:
                blocks.append(block)
            block = line.strip('\n')
        elif line[:2] == block[:2]:
            block += line[len(line.split(' ')[0]):].strip('\n')
        elif block[:2] == '.B':
            if line[0] != '#' and len(line) > 4:
                if line.find(':'):
                    line = line.split(':')[0]
                block.append(line.strip('\n'))
        else:
            if block:
                blocks.append(block)
                block = ''
    ts_array = [] 
    for block in blocks:
        path,values,units = '','',''
        if block[:2] == '.E':
            path,values,units = Parse_E(block,shef_crit)
        elif block[:2] == '.A':
            ts_array = Parse_A(block,shef_crit)
        elif block[:2] == '.B':
            ts_array = Parse_B(block,shef_crit)
        if path:
            if path not in data.keys():
                data[path] =  CDAPost.CDAPostTS(path,'LRL',units)
            for value in values:
                data[path].insertValue(value[0],value[1],value[2])
        if ts_array:
            for ts in ts_array:
                path,values,units = ts
                if path:
                    if path not in data.keys():
                        data[path] =  CDAPost.CDAPostTS(path,'LRL',units)
                    for value in values:
                        data[path].insertValue(value[0],value[1],value[2])
    return data

if __name__ == "__main__":
    import sys
    args = sys.argv
    if len(args) == 3:
        data = process_shef(args[1],args[2])
    else:
        print ("Usage:\nshef_loader.py shef_file shef_crit")
        sys.exit('Bad Syntax')
    api_key = ""
    api_url = ''
    for ts in data.keys():
        data[ts].post(api_key,api_url)
