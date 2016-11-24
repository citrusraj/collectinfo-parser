import json
import re
import os
import sys
import math
import copy
import shutil
import string
import logging
import section_filter_list

#FORMAT = '%(asctime)-15s -8s %(message)s'
#logging.basicConfig(format=FORMAT, filename= 'sec_par.log', level=logging.INFO)
FILTER_LIST = section_filter_list.FILTER_LIST

###############################################################################
########################### Section parser Util func ##########################
###############################################################################

def getSectionNameFromId(sec_id):
    raw_section_name = FILTER_LIST[sec_id]['raw_section_name']
    final_section_name = FILTER_LIST[sec_id]['final_section_name'] if 'final_section_name' in FILTER_LIST[sec_id] else ''
    return raw_section_name, final_section_name
    

def getByteMemFromStr(mem, suflen):
    if 'k' in mem or 'K' in mem:
        return getBytesFromFloat(mem, 10, suflen)
    elif 'm' in mem or 'M' in mem:
        return getBytesFromFloat(mem, 20, suflen)
    elif 'g' in mem or 'G' in mem:
        return getBytesFromFloat(mem, 30, suflen)
    elif 't' in mem or 'T' in mem:
        return getBytesFromFloat(mem, 40, suflen)
    else:
        return int(mem)

def getBytesFromFloat(mem, shift, suflen):
    memnum = float(mem[:-suflen])
    if mem == '0':
        return int(0)
    f, i = math.modf(memnum)
    num = 1 << shift
    totalmem = (i * num) + (f * num)
    return int(totalmem)


# Assumption - Always a valid number is passed to convert to integer/float
def strToNumber(number):
    try:
        return int(number)
    except ValueError:
        return float(number)


# Bool is represented as 'true' or 'false'
def strToBoolean(val):
    if not isBool(val):
        logging.warning("string passed for boolean conversion must be a boolean string true/false/yes/no")
        return
    if val.lower() in ['true', 'yes']:
        return True
    elif val.lower() in ['false', 'no']:
        return False


def isBool(val):
    return val.lower() in ['true', 'false', 'yes', 'no']


def typeCheckRawAll(nodes, sectionName, parsedOutput):
    for node in nodes:
        if sectionName in parsedOutput[node]:
            typeCheckFieldAndRawValues(parsedOutput[node][sectionName])

# Aerospike doesn't send float values
# pretty print and other cpu stats can send float
def typeCheckFieldAndRawValues(section):
    keys = []
    addKeys = []
    # ipRegex = "[0-9]{1,2,3}(\.[0-9]{1,2,3})*"
    for key in section:
        if isinstance(section[key], dict):
            typeCheckFieldAndRawValues(section[key])
        elif isinstance(section[key], list) and len(section[key]) > 0 and isinstance(section[key][0], dict):
            for item in section[key]:
                typeCheckFieldAndRawValues(item)
        
        elif isinstance(section[key], str):
            if type(section[key]) is int or type(section[key]) is bool or type(section[key]) is list:
                continue
            elif section[key] is None:
                logging.warning("Value for key " + key + " is Null")
                continue
            # Some numbers have a.b.c.d* format, which matches with IP address
            # So do a defensive check at starting.
            # All type of address stats and config should be string so continue
            # mesh-adderss, service-address.
            if 'addr' in key:
                continue

            # 3.9 config have ns name in some of the field names. {ns_name}-field_name
            # Thease fields are already under ns section, so no need to put ns_name again.
            # Remove ns name and put only filed name.
            if re.match(r'\{.*\}-.*', key):
                section[key.split('}-')[1]] = section.pop(key)

            # Handle format like (a,b,c) this is a valid number
            elif section[key].replace(",", "").isdigit():
                number = strToNumber(section[key].replace(",", ""))
                if number < sys.maxsize:
                    section[key] = number
                else:
                    keys.append(key)

            # Handle format (a.b.cd.s), its valid number.
            elif section[key].replace(".", "").isdigit():
                number = strToNumber(section[key].replace(".", ""))
                if number < sys.maxsize:
                    section[key] = number
                else:
                    keys.append(key)

            # Handle bool
            elif isBool(section[key]):
                section[key] = strToBoolean(section[key])

            # Handle format (-ab,c,f)
            elif section[key].lstrip("-").replace(",", "").isdigit():
                num = section[key].lstrip("-").replace(",", "")
                if num.isdigit():
                    number = strToNumber(num)
                    section[key] = -1 * number

        # continue for all int, float and bools
        else:
            continue

    for key in keys:
        section.pop(key, None)


# This should check only raw values.
# Aerospike doesn't send float values
# pretty print and other cpu stats can send float
def typeCheckBasicValues(section):
    malformedkeys = []
    # ipRegex = "[0-9]{1,2,3}(\.[0-9]{1,2,3})*"
    for key in section:
        if isinstance(section[key], dict):
            typeCheckBasicValues(section[key])

        elif isinstance(section[key], list) and len(section[key]) > 0 and isinstance(section[key][0], dict):
            for item in section[key]:
                typeCheckBasicValues(item)

        elif isinstance(section[key], str):
            if '.' in key:
                malformedkeys.append(key)
            if type(section[key]) is int or type(section[key]) is bool or type(section[key]) is list:
                continue
            elif section[key] is None:
                logging.info("Value for key " + key + " is Null")
                continue
            elif section[key] == 'N/E' or section[key] == 'n/e':
                logging.info("'N/E' for the field.")
                section[key] = None
                continue

            # Handle float of format (a.b), only 1 dot would be there.
            if section[key].replace(".", "", 1).isdigit():
                section[key] = strToNumber(section[key])

            # Handle bool
            elif isBool(section[key]):
                section[key] = strToBoolean(section[key])

            # Handle negative format (-ab,c,f)
            elif section[key].lstrip("-").isdigit():
                num = section[key].lstrip("-")
                if num.isdigit():
                    number = strToNumber(num)
                    section[key] = -1 * number

        # continue for all int, float and bools
        else:
            continue

    for key in malformedkeys:
        newkey = key.replace('.', '_')
        val = section[key]
        section.pop(key, None)
        section[newkey] = val



def getClusterSize(stats):
    totalStats = len(stats)
    for i in range(totalStats):
        if 'cluster_size' in stats[i]:
            cluster_size_list = stats[i].split( )
            for cluster_size in cluster_size_list:
                if isinstance(cluster_size, int):
                    return int(cluster_size)
            return int(0)



def identifyNamespace(content):
    sec_id = 'ID_2'
    raw_section_name, final_section_name = getSectionNameFromId(sec_id)

    if raw_section_name not in content:
        logging.warning(raw_section_name + " section not present.")
        return

    if len(content[raw_section_name]) > 1:
        logging.warning("More than one entries detected, There is a collision for this section: " + final_section_name)

    namespace = content[raw_section_name][0]
    del1 = False
    del2 = False
    nsList = []
    skiplines = 3
    for i in range(len(namespace)):
        if "Number of" in namespace[i] or "No. " in namespace[i]:
            # End of Section
            break
        if "~~~~Name" in Namespace[i]:
            del1 = True
            continue
        elif "=== NAME" in Namespace[i]:
            del2 = True
            continue
        if del1 or del2:
            if skiplines != 0:
                skiplines = skiplines - 1
                continue
        if del1:
            # Leave 3 lines and get unique list of ns
            ns = namespace[i].split()[0]
            nsList.append(ns)
        if del2:
            # Leave 3 lines and get unique list of ns (ip/ns)
            ns = namespace[i].split()[0].split('/')[1]
            nsList.append(ns)

    unqNsList = list(set(nsList))
    return unqNsList
        

def identifyNodes(content):
    nodes = getNodesFromLatencyInfo(content)
    if not nodes or len(nodes) == 0:
        logging.warning("couldn't find nodes from latency section.")
        nodes = getNodesFromNetworkInfo(content)
        if not nodes or len(nodes) == 0:
            logging.warning("couldn't find nodes from info_network section.")
        else:
            return nodes
    else:
        return nodes


def getNodesFromLatencyInfo(content):
    sec_id = 'ID_10'
    raw_section_name, final_section_name = getSectionNameFromId(sec_id)

    if raw_section_name not in content:
        logging.warning(raw_section_name + "section not present.")
        return

    if len(content[raw_section_name]) > 1:
        logging.warning("More than one entries detected, There is a collision for this section: " + final_section_name)

    latency = content[raw_section_name][0]
    delimiterCount = 0
    nodes = []
    for i in range(len(latency)):
        # the latency section can have delimiters - separates latency output for
        # read/write/writes_master/proxy etc.
        # Or it can have the data for a given node - we are looking to extract nodeid
        # from this data. Node's data appears after delimiter. Anything that appears
        # before delimiter can be omitted
        # Or it can have some data which is not interest to us.
        if "~~~~~~~" in latency[i] or "====" in latency[i]:
            delimiterCount += 1
        elif delimiterCount == 1 and 'time' not in latency[i]:
            # So far one delimiter.
            # 1 delimiter implies the subsequent lines contain data(nodeId) we are interested in.
            nodeId = getNodeId(latency[i])
            if nodeId is None:
                if len(latency[i]) > 2:
                    logging.warning("NodeId absent in latency section line" + latency[i])
                else:
                    logging.debug("NodeId not returned for an empty string")
            else:
                nodes.append(nodeId)
        elif delimiterCount == 2 and len(nodes) > 0:
            # We have come across two delimiters. NodeIds are present in between two delimiters.
            # So we infer here that all nodeIds are parsed. 
            logging.debug("Parsed all the nodes in latency, signing off" + str(nodes))
            return nodes
        else:
            # for the lines appearing before any delimiter
            continue

def getNodesFromNetworkInfo(content):
    sec_id = 'ID_49'
    raw_section_name, final_section_name = getSectionNameFromId(sec_id)

    if raw_section_name not in content:
        logging.warning(raw_section_name + "section not present.")
        return

    if len(content[raw_section_name]) > 1:
        logging.warning("More than one entries detected, There is a collision for this section: " + final_section_name)

    network_info = content[raw_section_name][0]
    del_found = False
    nodes = []
    skip_lines = 2
    for i in range(len(network_info)):
        if "~~~~~~~" in network_info[i] or  "====" in network_info[i]:
            del_found = True
            continue
        if "Number" in  network_info[i] or "No." in network_info[i]:
            logging.debug("Parsed all the nodes in info_network, signing off" + str(nodes))
            return nodes
        if  del_found:
            if skip_lines == 0:
                # Get node ip
                nodeLine = network_info[i].split(' ')
                if nodeLine[0] is ' ' or nodeLine[0] is '.':
                    logging.warning("NodeId absent in info_network section line" + network_info[i])
                    continue
                else:
                    nodes.append(nodeLine[0])

            else:
                skip_lines = skip_lines - 1



def updateSetAndBinCounts(parsedOutput, sectionName):
    for key in parsedOutput:
        currObj = parsedOutput[key][sectionName]
        all_sets = {}
        all_bins = {}
        all_ns = {}
        if 'Namespace' in currObj:
            all_ns = currObj['Namespace']
            for ns in all_ns.keys():
                nsObj = all_ns[ns]
                nsObj.update({'bins': {}})
                nsObj.update({'sets': {}})

        if 'Bin' in currObj.keys():
            all_bins = currObj['Bin']
            for sbin in all_bins.keys():
                binObj = all_bins[sbin]
                for ns in all_ns.keys():
                    nsObj = all_ns[ns]

                    #TODO: some file have different format, can not find ns_name check case 5518
                    if 'ns_name' not in nsObj['service'] or 'ns_name' not in binObj:
                        logging.warning("Could not find ns_name")
                        continue
                        #nsObj['service']['ns_name'] = 'UNKNOWN_NS'

                    if nsObj['service']['ns_name'] == binObj['ns_name']:
                        if 'num-bin-names' in binObj:
                            nsObj['service']['bin_count'] = binObj['num-bin-names']
                        # print(binObj)
                        nsObj['bins'].update({sbin: binObj})
                        # print(sbin + str(nsObj['bins']))
            currObj.pop('Bin')

        if 'Set' in currObj:
            all_sets = currObj['Set']
            for ns in all_ns.keys():
                all_ns[ns]['service']['set_count'] = 0
            for sset in all_sets.keys():
                setObj = all_sets[sset]
                for ns in all_ns.keys():
                    nsObj = all_ns[ns]

                    #TODO: some file have different format, can not find ns_name check case 5518
                    if 'ns_name' not in nsObj['service'] or 'ns_name' not in setObj:
                        logging.warning("Could not find ns_name")
                        #nsObj['service']['ns_name'] = 'UNKNOWN_NS'
                        continue

                    if nsObj['service']['ns_name'] == setObj['ns_name']:
                        nsObj['service']['set_count'] += 1
                        nsObj['sets'].update({sset: setObj})
            currObj.pop('Set')


def updateNsCount(parsedOutput):
    for nodeid in parsedOutput['nodes']:
        nodedata = parsedOutput['nodes'][nodeid]
        for sectname in nodedata:
            section = nodedata[sectname]
            if 'Namespace' in section:
                parsedOutput['cluster'].update({'ns_count': len(section['Namespace'])})
                break


def isSingleColumnFormat(section):
    logging.debug("inside nodes identification")
    length = len(section)
    for i in range(length):
        if "====" in section[i]:
            return True
        elif "~~~~~~" in section[i]:
            return False


def parseMultiColumnFormat(content, parsedOutput, sectionName):
    nsInitialized = False
    setInitialized = False
    binInitialized = False
    serviceInitialized = False
    entries = len(content)
    jsonArrays = []
    nsName = ""
    binName = ""
    setName = ""
    objKey = ""
    currentSection = ""
    currSecUpdated = False
    nodesPopulated = False

    for i in range(entries):
        # '~~~~~' is a delimiter - The line containing delimiter has
        # section of printconfig(Service|Network|<Namespace configuration>).
        # Do we want to process each namespace independently.
        # Processing together leads to overwriting existing values.
        # In Column format processing, config is present per node - not per namespace
        # TODO Non-uniformity in config from row format to column format.
        if "~" in content[i]:
            section = re.split("~*", content[i])[1]
            if 'Namespace' in section:
                currentSection = "Namespace"
                currSecUpdated = True
                nsName = section.split()[0]
                objKey = nsName
            elif 'Bin' in section:
                currentSection = "Bin"
                currSecUpdated = True
                binName = section.split()[0]
                objKey = binName
            elif 'Set' in section:
                currentSection = "Set"
                currSecUpdated = True
                setName = section.split()[0] + ' ' + section.split()[1]
                objKey = setName
            else:
                currentSection = "service"
                currSecUpdated = True
                logging.debug('current section is ' + currentSection)

        # Line that describes a section doesn't have ":" and we are not interested in it.
        elif ":" not in content[i]:
            continue
        else:
            # Otherwise Entries are in this format
            # <key> : <val_for_node1> <val_for_node2> <val_for_node3> <val_for_node4> <val_for_node5>
            # Below is an example entry.
            # "NODE:clf1003 clf1004 clf1005 clf1006 clf1007 \n",
            # Remove the spaces and get the entries in the  every line
            row = content[i].split(":", 1)
            # print warning if the len(row) != 2.
            if len(row) != 2:
                log.warning("MultiColumn Format Anamoly. A line contains unexpected entries : " + str(len(row)))
            key = row[0].split()[0]
            vals = row[1].split( )
            length = len(vals)
            # Here "NODE" is hardcoded to find the row containing nodeids.
            # TODO is there a better way to do it than hardcoding.

            if (key == "NODE" and not nodesPopulated):
                for nodeId in vals:
                    for key in parsedOutput:
                        if nodeId in key:
                            if sectionName not in parsedOutput[key]:
                                parsedOutput[key][sectionName] = {}
                            jsonArrays.append(parsedOutput[key][sectionName])
                            break
                nodesPopulated = True
            # if NODE is not present in line, the line can contain data for the currentSection.
            elif key != "NODE":
                # The current format of vals is [<val_for_node1>, <val_for_node2>, ...]
                # Parse from 0th entry and order the parsedOutput['nodeid'] json objects 
                # in the same order as the node ids in the give line.
                # Assumption - order of nodeids and values are same.
                index = 0
                for i in range(len(jsonArrays)):
                    # Order of nodeids and the values for each key is assumed to be same here.
                    if(index >= length):
                        logging.warning("Number of values do not match the cluster size, Values : " + str(length) + " index " + str(index))
                        break
                    if currentSection == "":
                        jsonArrays[i][key] = vals[index]
                    elif currentSection == "Namespace":
                        jsonArrays[i][currentSection][objKey]['service'][key] = vals[index]
                    elif currentSection == 'Set' or currentSection == 'Bin':
                        jsonArrays[i][currentSection][objKey][key] = vals[index]
                    else:
                        jsonArrays[i][currentSection][key] = vals[index]
                    index += 1
                    #warn in case len(val) > Cluster_size(no of nodes in the cluster).
                if index != length:
                    logging.warning("Number of values are lesser than cluster size, values : " + str(length))
            # If we crossed the section delimiter, initialize a new dictionary for this new section in node's dictionary.
            elif not currentSection:
                logging.warning("Data is present before a sub-section delimiter and thus data can't be associated with a sub-section")
                logging.warning(content[i])
            #   for i in range(len(jsonArrays)):
            #       jsonArrays[i][currentSection] = {}
            #   currSecUpdated = False
            if nodesPopulated and currentSection == "Namespace" and not nsInitialized:
                for i in range(len(jsonArrays)):
                    jsonArrays[i][currentSection] = {}
                nsInitialized = True
            if nodesPopulated and currentSection == "Set" and not setInitialized:
                for i in range(len(jsonArrays)):
                    jsonArrays[i][currentSection] = {}
                setInitialized = True
            if nodesPopulated and currentSection == "Bin" and not binInitialized:
                for i in range(len(jsonArrays)):
                    jsonArrays[i][currentSection] = {}
                binInitialized = True
            if currSecUpdated:
                if currentSection == "Namespace" or currentSection == "Set" or currentSection == "Bin":
                    obj = {}
                    #obj['service'] = {}
                    if currentSection == "Namespace":
                        obj['service'] = {}
                        obj['service']['ns_name'] = nsName
                    elif currentSection == "Set":
                        obj['set_name'] = setName
                    elif currentSection == "Bin":
                        obj['bin_name'] = binName
                    for i in range(len(jsonArrays)):
                        jsonArrays[i][currentSection].update({objKey:obj})
                elif currentSection == "service" and not serviceInitialized:
                    for i in range(len(jsonArrays)):
                        jsonArrays[i][currentSection] = {}
                    serviceInitialized = True
                currSecUpdated = False
    updateSetAndBinCounts(parsedOutput, sectionName)
    return

def initNodesForParsedJson(nodes, content, parsedOutput, sectionName):
    #nodes = list(parsedOutput.keys())
    #if len(nodes) == 0:
    #    nodes = identifyNodes(content)
    #    logging.info("Identified nodes: " + str(nodes))
    #    if not nodes:
    #        logging.warning("Latency or info_network section not available, NodeIds can't be identified")
    #        return 
    for node in nodes:
        if node not in parsedOutput:
            parsedOutput[node] = {}
        parsedOutput[node][sectionName] = {}


def parseSingleColumnFormat(content, parsedOutput, sectionName):
    nodes = list(parsedOutput.keys())
    output = {}
    jsonToFill = None
    entries = len(content)
    for i in range(entries):
        if "====" in content[i]:
            for node in nodes:
                if node in content[i]:
                    if sectionName not in parsedOutput[node]:
                        parsedOutput[node][sectionName] = {}
                    parsedOutput[node][sectionName]['service'] = {}
                    jsonToFill = parsedOutput[node][sectionName]['service']
                    break
        elif jsonToFill is not None:
            conf = content[i].split( )
            jsonToFill[conf[0]] = conf[-1].split('\n')[0]
    return output


def getHistogramName(latencyLine):
    if "====" in latencyLine or "~~~~" in latencyLine:
        # Assumptions : Histogram name consists of characters from a-z (cases ignored).
        # Name may contain "_" (underscore).
        name = re.findall('[a-z_]+',latencyLine, flags = re.IGNORECASE)[0]
        # First letter in the name is 'm' - invalid character. So ignore it.
        # To identify 'm' is an extra character not part of histogramname,
        # the latencyLine has '[1m' in it. 
        # identify using [1m - 
        if "[1m" in latencyLine:
            return name[1:]
        else:
            return name
    else:
        logging.warning("histogram name validator not present in the argument " + str(latencyLine))


# Given a line from latency Section, identifies all the keys and returns it as an array.
# Eg- [ ops/sec, >1ms, >8ms, >16ms] 
# Assumption - This line must contain time word in it.
# After removing spaces, few representation of histogram has ['node', 'time', 'ops/sec', ...] in it.
# and few other has ['time', 'ops/sec', ...]. We are interested in only values after ops/sec.
# TODO - Can there be a way to identify without hardcoding anything.
#      - Negation of time regex and absence of "===="/"~~~~" signifies this as a line containing
#      - histogram keys. But this logic is inference from something else. May not hold always.

def getHistogramKeys(latencyLine):
    if "time" in latencyLine.lower():
        names = latencyLine.split()
        index = 0
        for i in range(len(names)):
            names[i] = names[i].lower()
            if names[i] == "ops/sec":
                index = i
        # returnarr = names[index:]
        if len(names) > 4:
            return names[index:]
        else:
            logging.warning("Number of keys in histogram is less than four " + str(names))
    else:
        logging.warning("histogram keys validator not present in the argument " + str(latencyLine))


timeRegex = "\d{2}:\d{2}:\d{2}.*->\d{2}:\d{2}:\d{2}"


def getHistogramValues(latencyLine):
    global timeRegex
    if re.findall(timeRegex, latencyLine):
        values = re.split(timeRegex, latencyLine, maxsplit=1)[1].split()
        return values


# To identify if a line has nodeId, time is printed in the format 15:56:24-GMT->15:56:34"
# Do a regular expression search on the line.
def getNodeId(string):
    if len(string) > 2:
        global timeRegex
        if re.search(timeRegex, string):
            nodeId = string.split()[0]
            return nodeId
        else:
            logging.warning("The argument doesn't contain nodeId validator " + str(string))
            return None
    else:
        logging.debug("getNodeId string has only two character " + str(string))
        return None


###############################################################################
########################### Main section parser func ##########################
###############################################################################


def parseConfigSection(nodes, content, parsedOutput):
    sec_id = 'ID_5'
    raw_section_name, final_section_name = getSectionNameFromId(sec_id)

    logging.info("Parsing section: " + final_section_name)
    if not content:
        logging.warning("Null section json")
        return

    if raw_section_name not in content:
        logging.warning(raw_section_name + "section not present.")
        return

    if len(content[raw_section_name]) > 1:
        logging.warning("More than one entries detected, There is a collision for this section: " + final_section_name)
 
    configSection = content[raw_section_name][0]

    logging.debug("invoking format identifier")
    singleColumn = isSingleColumnFormat(configSection)

    initNodesForParsedJson(nodes, content, parsedOutput, raw_section_name)
    
    if singleColumn:
        parseSingleColumnFormat(configSection, parsedOutput, final_section_name)
    else:
        parseMultiColumnFormat(configSection, parsedOutput, final_section_name)
    typeCheckRawAll(nodes, final_section_name, parsedOutput)


def parseStatSection(nodes, content, parsedOutput):
    sec_id = 'ID_11'
    raw_section_name, final_section_name = getSectionNameFromId(sec_id)

    logging.info("Parsing section: " + final_section_name)
    if not content:
        logging.warning("Null section json")
        return

    if raw_section_name not in content:
        logging.warning(raw_section_name + "section not present.")
        return

    if len(content[raw_section_name]) > 1:
        logging.warning("More than one entries detected, There is a collision for this section: " + final_section_name)
 
    statSection = content[raw_section_name][0]
    
    logging.debug("invoking format identifier")
    singleColumn = isSingleColumnFormat(statSection)

    initNodesForParsedJson(nodes, content, parsedOutput, raw_section_name)
    
    if singleColumn:
        parseSingleColumnFormat(statSection, parsedOutput, final_section_name)
    else:
        parseMultiColumnFormat(statSection, parsedOutput, final_section_name)
    typeCheckRawAll(nodes, final_section_name, parsedOutput)


def parseLatencySection(nodes, content, parsedOutput):
    sec_id = 'ID_10'
    raw_section_name, final_section_name = getSectionNameFromId(sec_id)

    logging.info("Parsing section: " + final_section_name)
    if not content:
        logging.warning("Null section json")
        return

    if raw_section_name not in content:
        logging.warning(raw_section_name + "section not present.")
        return

    if len(content[raw_section_name]) > 1:
        logging.warning("More than one entries detected, There is a collision for this section: " + final_section_name)
    
    latency = content[raw_section_name][0]
    length = len(latency)
    histogram = ''

    initNodesForParsedJson(nodes, content, parsedOutput, raw_section_name) 

    for i in range(length):
        if "====" in latency[i] or "~~~~" in latency[i]:
            histogram = getHistogramName(latency[i])
            logging.info("Histogram name: " + histogram)
            for key in parsedOutput:
                parsedOutput[key][final_section_name][histogram] = {}
        elif 'time' in latency[i].lower():
            keys = getHistogramKeys(latency[i])
        else:
            nodeId = getNodeId(latency[i])
            logging.info("Got nodeId: " + str(nodeId))
            if nodeId is None and len(latency[i]) > 2:
                logging.warning("NodeId is None " + str(latency[i]))
                continue
            else:
                values = getHistogramValues(latency[i])
                if values is not None:
                    if len(keys) != len(values):
                        logging.warning("Histogram: number of keys and values do not match " + str(keys) + " " + str(values))
                        continue
                    else:
                        for i in range(len(values)):
                            if nodeId in parsedOutput:
                                parsedOutput[nodeId][final_section_name][histogram][keys[i]] = values[i]
                elif values is None:
                    if len(latency[i]) > 2:
                        logging.warning("getHistogram keys returned a NULL set for keys " + str(latency[i]))
                    else:
                        logging.debug("latency section contains an empty string")
    typeCheckRawAll(nodes, final_section_name, parsedOutput)


def parseSindexInfoSection(nodes, content, parsedOutput):
    sec_id = 'ID_51'
    raw_section_name, final_section_name = getSectionNameFromId(sec_id)

    logging.info("Parsing section: " + final_section_name)
    if not content:
        logging.warning("Null section json")
        return

    if raw_section_name not in content:
        logging.warning(raw_section_name + "section not present.")
        return

    if len(content[raw_section_name]) > 1:
        logging.warning("More than one entries detected, There is a collision for this section: " + final_section_name)
    
    
    sindexdata = {}
    sindexSection = content[raw_section_name][0]

    initNodesForParsedJson(nodes, content, parsedOutput, raw_section_name)
    
    # Get the starting of data
    # "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Secondary Index Information~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n",
    # "               Node          Index       Namespace         Set       Bin   State     Sync     Keys     Objects   si_accounted_memory     q           w          d       s   \n"
    # "                  .           Name               .           .      Type       .    State        .           .                     .     .           .          .       .   \n"
    # "10.103.208.193:3000   bid_creative   dsp_event_log   video_bid   NUMERIC      RW   synced   510      7616730                   18432     0     7635733      18967       0   \n"
    startIndex = 0
    for index in range(len(sindexSection)):
        if re.search('~~', sindexSection[index]):
            startIndex = index + 3
            break
    # Update sindex info for respective nodeid
    for index in range(len(sindexSection)):
        if index < startIndex:
            continue
        l = re.split('\ +', sindexSection[index])
        # End of section
        if len(l) < 5:
            break
        nodeId = l[0]
        if nodeId not in sindexdata:
            sindexdata[nodeId] = {}
            sindexdata[nodeId][final_section_name] = {}
            sindexdata[nodeId][final_section_name]['index'] = []
        indexObj = {}
        indexObj['index_name'] = l[1]
        indexObj['namespace'] = l[2]
        indexObj['set'] = l[3]
        indexObj['bin_type'] = l[4]
        indexObj['state'] = l[5]
        indexObj['sync_state'] = l[6]
        # Extra added info, previously not there.
        if len(l) > 8:
            indexObj['keys'] = l[7]
            indexObj['objects'] = l[8]
            indexObj['si_accounted_memory'] = l[9]
        sindexdata[nodeId][final_section_name]['index'].append(indexObj)

    # Update sindex count for respective nodes.
    for nodeId in sindexdata:
        sindexdata[nodeId][final_section_name]['index_count'] = len(sindexdata[nodeId][final_section_name]['index'])
        if nodeId in parsedOutput:
            parsedOutput[nodeId][final_section_name] = sindexdata[nodeId][final_section_name]
        else:
            logging.info("Node id not in nodes section: " + nodeId)

    typeCheckRawAll(nodes, final_section_name, parsedOutput)


def parseAsdversion(content, parsedOutput):
    sec_id_1 = 'ID_27'
    raw_section_name_1, final_section_name_1 = getSectionNameFromId(sec_id_1)

    sec_id_2 = 'ID_28'
    raw_section_name_2, final_section_name_2 = getSectionNameFromId(sec_id_2)

    logging.info("Parsing section: " + final_section_name_1)
    if not content:
        logging.warning("Null section json")
        return

    if raw_section_name_1 not in content and raw_section_name_2 not in content:
        logging.warning(raw_section_name_1 + " and " + raw_section_name_1 + " section not present.")
        return

    if len(content[raw_section_name]) > 1:
        logging.warning("More than one entries detected, There is a collision for this section: " + final_section_name)
 

    distro = {}
    distroFound = False
    toolFound = False
    amcFound = False
    verRegex = "[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{1,2}"
    for dist in ['build rpm', 'build dpkg']:
        if dist in content:
            distro = content[dist][0]
            for i in range(len(distro)):
                match = re.search(verRegex, distro[i])
                version = ' '
                if not match:
                    continue
                else:
                    version = distro[i][match.start():match.end()]
                if re.search("ser", distro[i]) and not re.search("tool", distro[i]):
                    parsedOutput['server-version'] = version
                    parsedOutput['package'] = dist
                    distroFound = True
                elif re.search("too", distro[i]):
                    parsedOutput['tool-version'] = version
                    toolFound = True
                elif re.search("amc", distro[i]) or re.search('management', distro[i]):
                    parsedOutput['amc-version'] = version
                    amcFound = True
                # in some cases the server version has format aerospike-3.5.14-27.x86_64.
                # so grep for aerospike, if any of the previous conditions were not met.
                elif (re.search("aerospike", distro[i]) or re.search('citrusleaf', distro[i])) \
                        and "x86_64" in distro[i] and 'client' not in distro[i]:
                    parsedOutput['server-version'] = version
                    parsedOutput['package'] = dist
                    distroFound = True
                else:
                    logging.debug("The line matches the regex but doesn't contain any valid versions " + distro[i])

    if not distroFound or not toolFound:
        logging.warning("Asd Version string not present in JSON.")

    
# output: {in_aws: AAA, instance_type: AAA}
def parseAWSDataSection(content, parsedOutput):
    sec_id_1 = 'ID_70'
    raw_section_name_1, final_section_name_1 = getSectionNameFromId(sec_id_1)

    sec_id_2 = 'ID_85'
    raw_section_name_2, final_section_name_2 = getSectionNameFromId(sec_id_2)

    logging.info("Parsing section: " + final_section_name_1)
    if not content:
        logging.warning("Null section json")
        return

    awsdata = {}
    field_count = 0
    total_fields = 2
    #if 'info_get_awsdata' not in content and 'Request metadata' not in content:
    #    logging.warning("`info_get_awsdata` or `Request metadata` section is not present in section json " + str(filepath))
    #    return

    awsSectionList = None
    # If both sections are present, select which has more number of lines.
    if raw_section_name_1 in content and raw_section_name_2 in content:
        if len(content[raw_section_name_1]) > len(content[raw_section_name_2]):
            awsSectionList = content[raw_section_name_1]
        else:
            awsSectionList = content[raw_section_name_2]

    elif raw_section_name_1 in content:
        awsSectionList = content[raw_section_name_2]
    elif raw_section_name_2 in content:
        awsSectionList = content[raw_section_name_2]
    else:
        logging.warning(raw_section_name_1 + " and " + raw_section_name_2 + " section is not present in section json.")
        return

    if len(awsSectionList) > 1:
        logging.warning("More than one entries detected, There is a collision for this section(aws_info).")

    awsSection = awsSectionList[0]

    for index, line in enumerate(awsSection):
        if field_count >= total_fields:
            break
        if 'inAWS' not in awsdata and re.search("This .* in AWS", line, re.IGNORECASE):
            awsdata['in_aws'] = True
            field_count += 1
            continue
        if 'inAWS' not in awsdata and re.search("not .* in aws", line, re.IGNORECASE):
            awsdata['in_aws'] = False
            field_count += 1
            continue
        if 'instance_type' not in awsdata and re.search("instance-type", line):
            awsdata['instance_type'] = (awsSection[index + 1]).split('\n')[0]
            field_count += 1
            if 'inAWS' not in awsdata:
                awsdata['in_aws'] = True
                field_count += 1
            continue
    parsedOutput[final_section_name_1] = awsdata


def parseLSBReleaseSection(content, parsedOutput):
    sec_id_1 = 'ID_25'
    raw_section_name_1, final_section_name_1 = getSectionNameFromId(sec_id_1)

    sec_id_2 = 'ID_26'
    raw_section_name_2, final_section_name_2 = getSectionNameFromId(sec_id_2)


    logging.info("Parsing section: " + final_section_name_1)
    if not content:
        logging.warning("Null section json")
        return

    if raw_section_name_1 not in content and raw_section_name_2 not in content:
        logging.warning(raw_section_name_1 + " and " + raw_section_name_1 + " section not present.")
        return

    lsbdata = {}
    lsbSectionNames = [raw_section_name_1, raw_section_name_2]
    for section in lsbSectionNames:
        lsbSectionList = None
        if section in content:
            logging.info("Section: " + section)
            lsbSectionList = content[section]
        else:
            continue

        if len(lsbSectionList) > 1:
            logging.warning("More than one entries detected, There is a collision for this section: " + section)
            ##return

        lsbSection = lsbSectionList[0]

        for index, line in enumerate(lsbSection):

            # "LSB Version:\t:base-4.0-amd64:base-4.0-noarch:core-4.0-amd64:
            # core-4.0-noarch:graphics-4.0-amd64:graphics-4.0-noarch:printing-4.0-amd64:
            # printing-4.0-noarch\n"
            # "Description:\tCentOS release 6.4 (Final)\n"
            matchobj = re.match(r'Description:\t(.*?)\n', line)
            if matchobj:
                lsbdata['description'] = matchobj.group(1)
                break
            # "['lsb_release -a']\n"
            # "['ls /etc|grep release|xargs -I f cat /etc/f']\n"
            # "Amazon Linux AMI release 2016.03\n"
            # "Red Hat Enterprise Linux Server release 6.7 (Santiago)\n"
            # "CentOS release 6.7 (Final)\n"
            if re.search('.* release [0-9]+', line):
                lsbdata['description'] = line.split('\n')[0]
                break
            # Few formats have only PRETTY_NAME, so need to add this condition.
            # "PRETTY_NAME=\"Ubuntu 14.04.2 LTS\"\n"
            matchobj = re.match(r'PRETTY_NAME=\"(.*?)\"\n', line)
            if matchobj:
                lsbdata['description'] = matchobj.group(1)
                break
    parsedOutput[final_section_name_1] = lsbdata


def parseTopSection(content, parsedOutput):
    sec_id = 'ID_36'
    raw_section_name, final_section_name = getSectionNameFromId(sec_id)

    logging.info("Parsing section: " + final_section_name)
    if not content:
        logging.warning("Null section json")
        return

    if raw_section_name not in content:
        logging.warning(raw_section_name + "section not present.")
        return

    if len(content[raw_section_name]) > 1:
        logging.warning("More than one entries detected, There is a collision for this section: " + final_section_name)
 

    topdata = {'uptime': {}, 'tasks': {}, 'cpu_utilization': {}, 'ram': {}, 'swap': {}, 'asd_process': {}, 'xdr_process': {}}
    topSection = content[raw_section_name][0]
    asd_flag = False
    xdr_flag = False
    kib_format = False
    for index, line in enumerate(topSection):
        line = line.strip()
        if re.search('top -n3 -b', line):
            continue

        if 'KiB' in line:
            kib_format = True

        # Match object to get uptime in days.
        # "top - 18:56:45 up 103 days, 13:00,  2 users,  load average: 1.29, 1.34, 1.35\n"
        matchobj_1 = re.match(r'.*up (.*?) days.*', line)
        # Match object to get total task running.
        # "Tasks: 149 total,   1 running, 148 sleeping,   0 stopped,   0 zombie\n"
        matchobj_2 = re.match(r'Tasks.* (.*?) total.* (.*?) running.* (.*?) sleeping.*', line)
        # Match object to get cpu utilization info.
        # "%Cpu(s): 11.3 us,  1.0 sy,  0.0 ni, 85.0 id,  1.7 wa,  0.0 hi,  0.7 si,  0.3 st\n"
        matchobj_3 = re.match(r'.*Cpu.* (.*?).us.* (.*?).sy.* (.*?).ni.* (.*?).id.* (.*?).wa.* (.*?).hi.* (.*?).si.* (.*?).st.*', line)
        # Match object to get RAM info.
        # "KiB Mem:  62916356 total, 54829756 used,  8086600 free,   194440 buffers\n"
        matchobj_4 = re.match(r'.*Mem:.* (.*?) total.* (.*?) used.* (.*?) free.* (.*?) buffers.*', line)
        # Match object to get Swap Mem info.
        # "KiB Swap:        0 total,        0 used,        0 free. 52694652 cached Mem\n"
        matchobj_5 = re.match(r'.*Swap:.* (.*?) total.* (.*?) used.* (.*?) free.* (.*?) cached.*', line)

        if matchobj_1:
            topdata['uptime']['days'] = matchobj_1.group(1)
        elif matchobj_2:
            topdata['tasks']['total'] = matchobj_2.group(1)
            topdata['tasks']['running'] = matchobj_2.group(2)
            topdata['tasks']['sleeping'] = matchobj_2.group(3)
        elif matchobj_3:
            topdata['cpu_utilization']['user'] = matchobj_3.group(1)
            topdata['cpu_utilization']['system'] = matchobj_3.group(2)
            topdata['cpu_utilization']['ni'] = matchobj_3.group(3)
            topdata['cpu_utilization']['ideal'] = matchobj_3.group(4)
            topdata['cpu_utilization']['wait'] = matchobj_3.group(5)
            topdata['cpu_utilization']['hi'] = matchobj_3.group(6)
            topdata['cpu_utilization']['si'] = matchobj_3.group(7)
            topdata['cpu_utilization']['st'] = matchobj_3.group(8)
        elif matchobj_4:
            topdata['ram']['total'] = getByteMemFromStr(matchobj_4.group(1), 1)
            topdata['ram']['used'] = getByteMemFromStr(matchobj_4.group(2), 1)
            topdata['ram']['free'] = getByteMemFromStr(matchobj_4.group(3), 1)
            topdata['ram']['buffers'] = getByteMemFromStr(matchobj_4.group(4), 1)
        elif matchobj_5:
            topdata['swap']['total'] = getByteMemFromStr(matchobj_5.group(1), 1)
            topdata['swap']['used'] = getByteMemFromStr(matchobj_5.group(2), 1)
            topdata['swap']['free'] = getByteMemFromStr(matchobj_5.group(3), 1)
            topdata['swap']['cached'] = getByteMemFromStr(matchobj_5.group(4), 1)
        else:
            # Break, If we found data for both process.
            # Also break if it chacked more the top 15 process.
            if (asd_flag and xdr_flag) or index > 25:
                break
            # "  PID USER      PR  NI    VIRT    RES    SHR S  %CPU %MEM     TIME+ COMMAND\n"
            # "26937 root      20   0 59.975g 0.049t 0.048t S 117.6 83.9 164251:27 asd\n"
            if not asd_flag and re.search('asd', line):
                asd_flag = True
                l = re.split('\ +', line)
                topdata['asd_process']['virtual_memory'] = l[4]
                topdata['asd_process']['resident_memory'] = l[5]
                topdata['asd_process']['shared_memory'] = l[6]
                for field in topdata['asd_process']:
                    topdata['asd_process'][field] = getByteMemFromStr(topdata['asd_process'][field], 1)
            elif not xdr_flag and re.search('xdr', line):
                xdr_flag = True
                l = re.split('\ +', line)
                topdata['xdr_process']['virtual_memory'] = l[4]
                topdata['xdr_process']['resident_memory'] = l[5]
                topdata['xdr_process']['shared_memory'] = l[6]
                for field in topdata['xdr_process']:
                    topdata['xdr_process'][field] = getByteMemFromStr(topdata['xdr_process'][field], 1)
    if kib_format:
        
        for key in topdata['ram']:
            topdata['ram'][key] = topdata['ram'][key] * 1024
        for key in topdata['swap']:
            topdata['swap'][key] = topdata['swap'][key] * 1024
    parsedOutput[final_section_name] = topdata




def getMemInNumber(memStr):
    memNum = memStr[:-1] if memStr[-1] == 'k' else memStr
    return memNum


#output: {kernel_name: AAA, nodename: AAA, kernel_release: AAA}
def parseUnameSection(content, parsedOutput):
    sec_id = 'ID_24'
    raw_section_name, final_section_name = getSectionNameFromId(sec_id)

    logging.info("Parsing section: " + final_section_name)
    if not content:
        logging.warning("Null section json")
        return

    if raw_section_name not in content:
        logging.warning(raw_section_name + "section not present.")
        return

    if len(content[raw_section_name]) > 1:
        logging.warning("More than one entries detected, There is a collision for this section: " + final_section_name)
 

    unamedata = {}
    unameSection = content[raw_section_name][0]

    for line in unameSection:
        if re.search('uname -a', line):
            continue
        # "Linux e-asmem-01.ame.admarketplace.net 2.6.32-279.el6.x86_64 #1 SMP Fri Jun 22 12:19:21 UTC 2012 x86_64 x86_64 x86_64 GNU/Linux\n"
        l = re.split('\ +', (line.split('#')[0]))
        unamedata['kernel_name'] = l[0]
        unamedata['nodename'] = l[1]
        unamedata['kernel_release'] = l[2]
        break
    parsedOutput[final_section_name] = unamedata


# output: {key: val..........}
def parseMeminfoSection(content, parsedOutput):
    sec_id = 'ID_92'
    raw_section_name, final_section_name = getSectionNameFromId(sec_id)

    logging.info("Parsing section: " + final_section_name)
    if not content:
        logging.warning("Null section json")
        return

    if raw_section_name not in content:
        logging.warning(raw_section_name + "section not present.")
        return

    if len(content[raw_section_name]) > 1:
        logging.warning("More than one entries detected, There is a collision for this section: " + final_section_name)

    meminfodata = {}
    meminfoSection = content[raw_section_name][0]
    
    # If this section is not empty then there would be more than 5-6 lines, defensive check.
    if len(meminfoSection) < 3:
        logging.info("meminfo section seems empty.")
        return

    start_section_flag = False
    for line in meminfoSection:
        # If line is a newline char, skip it. line size 4 (defensive check)
        if len(line) < 4 or line == '\n':
            continue
        if not start_section_flag and (re.search('meminfo', line) or ' kB' in line):
            start_section_flag = True
            continue
        if start_section_flag:
            # "MemTotal:       32653368 kB\n",
            keyval = line.split(':')
            #meminfodata[keyval[0]] = (re.split('\ +',(keyval[1]).strip()))[0]
            meminfodata[keyval[0]] = int(keyval[1].split()[0]) * 1024
    parsedOutput[final_section_name] = meminfodata



### "hostname\n",
### "rs-as01\n",
# output: {hostname: [hnlist]}
def parseHostnameSection(content, parsedOutput):
    sec_id = 'ID_22'
    raw_section_name, final_section_name = getSectionNameFromId(sec_id)

    logging.info("Parsing section: " + final_section_name)
    if not content:
        logging.warning("Null section json")
        return

    if raw_section_name not in content:
        logging.warning(raw_section_name + "section not present.")
        return

    if len(content[raw_section_name]) > 1:
        logging.warning("More than one entries detected, There is a collision for this section: " + final_section_name)
 
    hnamedata = {}
    hnameSection = content[raw_section_name][0]

    for line in hnameSection:
        if line == '\n' or line == '.' or 'hostname' in line:
            continue
        else:
            hnamedata['hosts'] = line.rstrip().split()
            break

    parsedOutput[final_section_name] = hnamedata


### "Filesystem             Size  Used Avail Use% Mounted on\n",
### "/dev/xvda1             7.8G  1.6G  5.9G  21% /\n",
### "none                   4.0K     0  4.0K   0% /sys/fs/cgroup\n",

# output: [{name: AAA, size: AAA, used: AAA, avail: AAA, %use: AAA, mount_point: AAA}, ....]
def parseDfSection(content, parsedOutput):
    sec_id = 'ID_38'
    raw_section_name, final_section_name = getSectionNameFromId(sec_id)

    logging.info("Parsing section: " + final_section_name)
    if not content:
        logging.warning("Null section json")
        return

    if raw_section_name not in content:
        logging.warning(raw_section_name + "section not present.")
        return

    if len(content[raw_section_name]) > 1:
        logging.warning("More than one entries detected, There is a collision for this section: " + final_section_name)
 

    dfData = []
    tokCount = 0
    startSec = False
    kb_size = False

    dfSection = content[raw_section_name][0]

    for line in dfSection:

        if line.rstrip() == '':
            break
        if 'Filesystem' in line:
            startSec = True
            tokList = line.rstrip().split()
            tokCount = len(tokList)
            continue
        if '1K-block' in line:
            kb_size = True

        if startSec:
            tokList = line.rstrip().split()
            if (len(tokList) + 1 != tokCount):
                break

            fileSystem = {}
            fileSystem['name'] = tokList[0]
            fileSystem['size'] = getByteMemFromStr(tokList[1], 1)
            fileSystem['used'] = getByteMemFromStr(tokList[2], 1)
            fileSystem['avail'] = getByteMemFromStr(tokList[3], 1)
            fileSystem['%use'] = tokList[4].replace('%', '')
            fileSystem['mount_point'] = tokList[5]
            if kb_size:
                fileSystem['size'] = fileSystem['size'] * 1024
            dfData.append(fileSystem)

    parsedOutput[final_section_name] = {}
    parsedOutput[final_section_name]['Filesystems'] = dfData


### "             total       used       free     shared    buffers     cached\n",
### "Mem:         32068      31709        358          0         17      13427\n",
### "-/+ buffers/cache:      18264      13803\n",
### "Swap:         1023        120        903\n",

# output: {mem: {}, buffers/cache: {}, swap: {}} 
def parseFreeMSection(content, parsedOutput):
    sec_id = 'ID_37'
    raw_section_name, final_section_name = getSectionNameFromId(sec_id)

    logging.info("Parsing section: " + final_section_name)
    if not content:
        logging.warning("Null section json")
        return

    if raw_section_name not in content:
        logging.warning(raw_section_name + "section not present.")
        return

    if len(content[raw_section_name]) > 1:
        logging.warning("More than one entries detected, There is a collision for this section: " + final_section_name)
 

    freeMData = {}
    tokList = []
    startSec = False

    freeMSection = content[raw_section_name][0]

    for line in freeMSection:
        if 'total' in line and 'used' in line and 'free' in line:
            tokList = line.rstrip().split()
            startSec = True

        if startSec and 'Mem:' in line:
            dataList = line.rstrip().split()

            memObj = {}
            for idx, val in enumerate(tokList):
                memObj[val] = dataList[idx + 1]
    
            freeMData['mem'] = memObj
            continue
        
        if startSec and '-/+ buffers/cache:' in line:
            dataList = line.rstrip().split()

            bufferObj = {}
            bufferObj[tokList[1]] = dataList[2]
            bufferObj[tokList[2]] = dataList[3]

            freeMData['buffers/cache'] = bufferObj
            continue

        if startSec and 'Swap:' in line:
            dataList = line.rstrip().split()

            swapObj = {}
            swapObj[tokList[0]] = dataList[1]
            swapObj[tokList[1]] = dataList[2]
            swapObj[tokList[2]] = dataList[3]

            freeMData['swap'] = swapObj
            continue
    parsedOutput[final_section_name] = freeMData
    

### "iostat -x 1 10\n",
### "Linux 2.6.32-279.el6.x86_64 (bfs-dl360g8-02) \t02/02/15 \t_x86_64_\t(24 CPU)\n",
### "avg-cpu:  %user   %nice %system %iowait  %steal   %idle\n",
### "           0.78    0.00    1.44    0.26    0.00   97.51\n",
### "\n",
### "Device:         rrqm/s   wrqm/s     r/s     w/s   rsec/s   wsec/s avgrq-sz avgqu-sz   await  svctm  %util\n",
### "sdb               0.00     4.00    0.00    4.00     0.00    64.00    16.00     0.02    5.75   4.00   1.60\n",


# output: [{avg-cpu: {}, device_stat: {}}, .........]
def parseIOstatSection(content, parsedOutput):
    sec_id = 'ID_43'
    raw_section_name, final_section_name = getSectionNameFromId(sec_id)

    logging.info("Parsing section: " + final_section_name)
    if not content:
        logging.warning("Null section json")
        return

    if raw_section_name not in content:
        logging.warning(raw_section_name + "section not present.")
        return

    if len(content[raw_section_name]) > 1:
        logging.warning("More than one entries detected, There is a collision for this section: " + final_section_name)
 

    iostatSection = content[raw_section_name][0]

    # Create a List of all instances of iostat data.
    sectionList = []
    start = False
    section = []
    for line in iostatSection:
        if 'avg-cpu' in line and 'user' in line:
            if start:
                sectionList.append(section)
                section = []
            start = True
        section.append(line)
    sectionList.append(section)

    iostatData = []
    tokList = []
    startSec = False

    avgcpuLine = False
    tok_cpuline = []
    deviceLine = False
    tok_deviceline = []

    # Iterate over all instances and create list of maps
    for iostatSection in sectionList:
        sectionData = {}
        cpuobj = {}
        deviceobj = {}
        for line in iostatSection:
            if 'avg-cpu' in line and 'user' in line:
                startSec = True
                avgcpuLine = True
                tok_cpuline = line.rstrip().split()
                continue
            
            if 'Device:' in line and 'rrqm/s' in line:
                avgcpuLine = False
                deviceLine = True
                tok_deviceline = line.rstrip().split()
                continue


            if avgcpuLine:
                dataList = line.rstrip().split()
                if len(dataList) + 1 != len(tok_cpuline):
                    continue

                for idx, val in enumerate(dataList):
                    cpuobj[tok_cpuline[idx + 1]] = val
                continue

            if deviceLine:
                dataList = line.rstrip().split()
                if len(dataList) != len(tok_deviceline):
                    continue

                deviceobj[tok_deviceline[0].replace(':', '')] = dataList[0]
                for idx, val in enumerate(dataList):
                    if idx == 0:
                        continue
                    deviceobj[tok_deviceline[idx]] = val

        sectionData['avg-cpu'] = cpuobj
        sectionData['device_stat'] = deviceobj
        iostatData.append(sectionData)

    parsedOutput[final_section_name] = {}
    parsedOutput[final_section_name]['iostats'] = iostatData


def parseInterruptsSection(content, parsedOutput):
    sec_id = 'ID_93'
    raw_section_name, final_section_name = getSectionNameFromId(sec_id)

    logging.info("Parsing section: " + final_section_name)
    if not content:
        logging.warning("Null section json")
        return

    if raw_section_name not in content:
        logging.warning(raw_section_name + "section not present.")
        return

    if len(content[raw_section_name]) > 1:
        logging.warning("More than one entries detected, There is a collision for this section: " + final_section_name)
 

    irqSection = content[raw_section_name][0]

    tokList = []
    intList = []
    for line in irqSection:
        if 'cat /proc' in line or line == '\n':
            continue
        if 'CPU' in line:
            cpu_tok = line.rstrip().split()
            continue
        if 'TxRx' in line:

            tokList = line.rstrip().split()
            device_name = tokList[-1]
            int_type = tokList[-2]
            int_id = tokList[0]
            cpu_list = tokList[1:-2]

            dev_obj = {}
            int_obj = {}
            int_obj['interrupts'] = {}
            for idx, cpu in enumerate(cpu_tok):
                int_obj['interrupts'][cpu] = cpu_list[idx]

            dev_obj[device_name] = int_obj
            dev_obj['interrupt_id'] = int_id
            dev_obj['interrupt_type'] = int_type
            intList.append(dev_obj)

    parsedOutput[final_section_name] = {}
    parsedOutput[final_section_name]['device_interrupts'] = intList



########################################################################
################## Wraper func for parsing sections ####################
########################################################################


def parseSysSection(sectionList, content, parsedOutput):
    logging.info("Parse sys stats.")
    for section in sectionList:
        logging.info("Parsing section: " + section)

        if section == 'top':
            parseTopSection(content, parsedOutput)

        elif section == 'lsb':
            parseLSBReleaseSection(content, parsedOutput)

        elif section == 'uname':
            parseUnameSection(content, parsedOutput)

        elif section == 'meminfo':
            parseMeminfoSection(content, parsedOutput)

        elif section == 'awsdata':
            parseAWSDataSection(content, parsedOutput)

        elif section == 'hostname':
            parseHostnameSection(content, parsedOutput)

        elif section == 'df':
            parseDfSection(content, parsedOutput)

        elif section == 'free-m':
            parseFreeMSection(content, parsedOutput)

        elif section == 'iostat':
            parseIOstatSection(content, parsedOutput)

        elif section == 'interrupts':
            parseInterruptsSection(content, parsedOutput)

        else:
            logging.warning("Section unknown, can not be parsed. Check SECTION_NAME_LIST.")
        
        if section in parsedOutput:
            paramMap = {section: parsedOutput[section]}
            typeCheckBasicValues(paramMap)
            parsedOutput[section] = copy.deepcopy(paramMap[section])
    
    logging.info("Converting basic raw string vals to original vals.")


def parseAsSection(sectionList, content, parsedOutput):
    # Parse As stat
    logging.info("Parse As stats.")
    nodes = identifyNodes(content)
    if not nodes:
        logging.warning("Node can't be identified. Can not parse")
        return

    for section in sectionList:
        logging.info("Parsing section: " + section)

        if section == 'statistics':
            parseStatSection(nodes, content, parsedOutput)

        elif section == 'config':
            parseConfigSection(nodes, content, parsedOutput)

        elif section == 'latency':
            parseLatencySection(nodes, content, parsedOutput)

        elif section == 'sindex_info':
            parseSindexInfoSection(nodes, content, parsedOutput)

        else:
            logging.warning("Section unknown, can not be parsed. Check SECTION_NAME_LIST.")
        
        if section in parsedOutput:
            paramMap = {section: parsedOutput[section]}
            typeCheckBasicValues(paramMap)
            parsedOutput[section] = copy.deepcopy(paramMap[section])

    logging.info("Converting basic raw string vals to original vals.")


