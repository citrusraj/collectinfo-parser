import section_parser
import cinfo_parser
import section_filter_list
import logging

AS_SECTION_NAME_LIST = section_filter_list.AS_SECTION_NAME_LIST
SYS_SECTION_NAME_LIST = section_filter_list.SYS_SECTION_NAME_LIST
SECTION_FILTER_LIST = section_filter_list.FILTER_LIST

def getSectionListForParsing(outmap, available_section):
    final_section_list = []
    outmap_section_list = []
    if 'section_ids' not in outmap:
        logging.warning("`section_ids` section missing in section_json.")
        return final_section_list
    for section_id in outmap['section_ids']:
        section = SECTION_FILTER_LIST[section_id]
        if 'final_section_name' in section:
            sec_name = ''
            if 'parent_section_name' in section:
                sec_name = section['parent_section_name'] + '.' + section['final_section_name']
            else:
                sec_name = section['final_section_name']
            outmap_section_list.append(sec_name)
    final_section_list = list(set(outmap_section_list).intersection(available_section))
    return final_section_list

def match_nodeip(sys_map, known_nodes):
    node = 'UNKNOWN'
    found = False
    host = ''

    uname_host = ''
    if 'uname' in sys_map:
        uname_host = sys_map['uname']['nodename']
    for ip in known_nodes:
        if uname_host in ip:
            host = ip
            found = True
            
    if not found and 'hostname' in sys_map:
        sys_hosts = sys_map['hostname']['hosts']
        for sys_host in sys_hosts:
            for ip in known_nodes:
                if sys_host in ip:
                    host = ip
                    found = True
    if found is True:
        node = host
    return node 

def parseAllStatsCinfo(filepaths, parsedOutput, force = False):
    outmap = {}
    timestamp = ''
    for filepath in filepaths:
        if timestamp == '':
            timestamp = cinfo_parser.get_timestamp_from_file(filepath)
        cinfo_parser.extract_validate_filter_section_from_file(filepath, outmap, force)
        
    as_map = {}
    as_section_list = getSectionListForParsing(outmap, AS_SECTION_NAME_LIST)
    section_parser.parseAsSection(as_section_list, outmap, as_map)
    
    sys_map = {}
    sys_section_list = getSectionListForParsing(outmap, SYS_SECTION_NAME_LIST)
    section_parser.parseSysSection(sys_section_list, outmap, sys_map)
    
    cluster_name = section_parser.getClusterName(as_map)
    if cluster_name is None:
        cluster_name = 'null'
    
    if timestamp not in parsedOutput:
        parsedOutput[timestamp] = {}
        parsedOutput[timestamp][cluster_name] = {}
    
    nodemap = parsedOutput[timestamp][cluster_name]
    # Insert as_stat
    for nodeid in as_map:
        nodemap[nodeid] = {}
        nodemap[nodeid]['as_stat'] = as_map[nodeid]
    
    # Insert sys_stat
    nodes = nodemap.keys()
    node = match_nodeip(sys_map, nodes)
    if node in nodemap:
        nodemap[node]['sys_stat'] = sys_map
    else:
        nodemap[node] = {}
        nodemap[node]['sys_stat'] = sys_map

def parseAllStatsCinfoOld(filepath, parsedOutput, force = False):
    parseAllAsStatsCinfo(filepath, parsedOutput, force)
    parseAllSysStatsCinfo(filepath, parsedOutput, force)


def parseAllAsStatsCinfo(filepath, parsedOutput, force = False):
    # Parse collectinfo and create intermediate section_map
    logging.info("Parsing All aerospike stat sections.")
    outmap = {}
    cinfo_parser.extract_validate_filter_section_from_file(filepath, outmap, force)

    section_list = getSectionListForParsing(outmap, AS_SECTION_NAME_LIST)

    logging.info("Parsing sections: " + str(section_list))
    section_parser.parseAsSection(section_list, outmap, parsedOutput)



def parseAllSysStatsCinfo(filepath, parsedOutput, force = False):
    # Parse collectinfo and create intermediate section_map
    logging.info("Parsing All sys stat sections.")
    outmap = {}
    cinfo_parser.extract_validate_filter_section_from_file(filepath, outmap, force)
    section_list = getSectionListForParsing(outmap, SYS_SECTION_NAME_LIST)

    logging.info("Parsing sections: " + str(section_list))
    section_parser.parseSysSection(section_list, outmap, parsedOutput)



def parseAsStatsCinfo(filepath, parsedOutput, sectionList, force = False):
    # Parse collectinfo and create intermediate section_map
    logging.info("Parsing As stat sections.")
    outmap = {}
    cinfo_parser.extract_validate_filter_section_from_file(filepath, outmap, force)

    section_parser.parseAsSection(sectionList, outmap, parsedOutput)    



def parseSysStatsCinfo(filepath, parsedOutput, sectionList, force = False):
    # Parse collectinfo and create intermediate section_map
    logging.info("Parsing system stat sections.")
    outmap = {}
    cinfo_parser.extract_validate_filter_section_from_file(filepath, outmap, force)

    section_parser.parseSysSection(sectionList, outmap, parsedOutput)



def parseSysStatsLiveCmd(cmdName, cmdOutput, parsedOutput):
    # Parse live cmd output and create outmap
    outmap = {}
    cinfo_parser.extract_section_from_live_cmd(cmdName, cmdOutput, outmap)
    sectionList = []
    sectionList.append(cmdName)
    section_parser.parseSysSection(sectionList, outmap, parsedOutput)


def test():
    parsedOutput = {}
    dir = '/tmp/tmp/collectInfo_20170202_100252'
    #filepaths = [dir + '/20170202_100252_aerospike.conf', dir + '/20170202_100252_ascollectinfo.log', dir + '/20170202_100252_sysinfo.log']
    parseAllStatsCinfo([dir + '/20170202_100252_aerospike.conf'], parsedOutput)
    print(parsedOutput)
    parseAllStatsCinfo([dir + '/20170202_100252_ascollectinfo.log'], parsedOutput)
    print(parsedOutput)    
    parseAllStatsCinfo([dir + '/20170202_100252_sysinfo.log'], parsedOutput)
    print(parsedOutput)
    