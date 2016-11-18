import os
import json
import re
import logging
import section_filter_list


# Section filter list.
# Param regex_new: regex for collectinfos having delimiter.
# Param regex_old: regex for collectinfos, not having delimiter.
# FILTER_LIST = section_filter_list.FILTER_LIST
# SKIP_LIST = section_filter_list.SKIP_LIST
DELIMIT_REGEX = 'regex_new'
NON_DELIMIT_REGEX = 'regex_old'
SECTION_DELIMITER = 'ASCOLLECTINFO'

MIN_VALIDATOR_STRING = 2
COLLECTINFO_START_LINE_MAX = 4
SECTION_DETECTION_LINE_MAX = 2
MIN_SECTIONS_IN_COLLECT_INFO = 2

FILTER_LIST = section_filter_list.FILTER_LIST
SKIP_LIST = section_filter_list.SKIP_LIST

#FORMAT = '%(asctime)-15s -8s %(message)s'
#logging.basicConfig(format=FORMAT, filename= 'cinfo_par.log', level=logging.INFO)

# Remove disabled filter section from result dictionary.
# Param outmap: Dictionary having parsed section entries.
# Param filter_list: Parsed section filter list.
def filter_processed_cinfo(outmap, filter_list):
    logging.info("Removing disabled filter section...")
    for index, filter_obj in enumerate(filter_list):
        if filter_obj['enable'] is False:
            # Remove that key from map
            try:
                logging.debug("Removing filter section from outmap: " + str(filter_obj['section']))
                del outmap[filter_obj['section']]
            except KeyError:
                pass


# Check if given file contain delimiter in file.
# Param path: Path of file been checked.
# Param delimiter: file delimiter, checked to be present.
def is_delimiter_collectinfo(path, delimiter):
    with open(path, 'r') as inf:
        index = 0
        fileline = ''
        while(True):
            try:
                fileline = inf.readline()
                if fileline == '':
                    break
            except UnicodeDecodeError as e:
                logging.warning('Error at: ' + fileline)
                logging.warning(e)
                continue
            # Check only till "COLLECTINFO_START_LINE_MAX" number of lines.
            if index >= COLLECTINFO_START_LINE_MAX:
                break
            if re.search(delimiter, fileline):
                return True
            index += 1
        return False


# Update section map, report for collisions.
# Param newcinfo: collectinfo file
# Param key: key to be added in Map
# Param value: value to be added in map
# Param outmap: Map having parsed sections
def updateMap(newcinfo, key, value, outmap, skip_list, force):
    vallist = []
    same_section = False
    if key in outmap.keys():
        preval = outmap[key]
        logging.warning("There is a collision for section: " + key)
        if newcinfo:
            # Skip section which are repeated in collectinfo with some variable name.
            # hist-dump:ns=<ns_name>;hist-name=<ttl|objsz>
            for sec in skip_list:
                if sec in key:
                    same_section = True
            # TODO Enhance this for old cinfo also by adding filter line in section
            for section in preval:
                if section[0].strip() == value[0].strip() or \
                        'log' in str(section[:2]) and 'log' in str(value[:2]):
                    same_section = True
            if not same_section:
                logging.error("collision between two different sections, There could be new section added. Please check logs")
                logging.info("old_sections: " + str(preval[:2]))
                logging.info("new_section: " + str(value[:2]))
                if force is False:
                    raise Exception("collision between two different sections, There could be new section added. Please check logs")
        vallist.extend(preval)

    # This would append all colliding section in a list
    vallist.append(value)
    outmap[key] = vallist

def extract_validate_filter_section_from_file(filepath, outmap, force):
    filter_list = FILTER_LIST
    logging.info("Creating section json. parse, validate, filter sections.")
    parse_all = True
    section_count = extract_section_from_file(filepath, parse_all, outmap, force)
    validateSectionCount(section_count, outmap, force)
    filter_processed_cinfo(outmap, filter_list)

def extract_section_from_file(filepath, parse_all, outmap, force):
    logging.info("Extract sections from collectinfo file.")
    delimit = SECTION_DELIMITER
    delimit_regx = DELIMIT_REGEX
    non_delimit_regx = NON_DELIMIT_REGEX
    filter_list = FILTER_LIST
    skip_list = SKIP_LIST

    if not os.path.exists(filepath):
        logging.warning("collectinfo doesn't exist at path: " + filepath)
        return 0

    if is_delimiter_collectinfo(filepath, delimit):
        logging.info("New collectinfo version delimit 'ASCOLLECTINFO': " + filepath)
        section_count = section_count_fun(filepath, delimit)
        extract_section_from_new_cinfo(filepath, filter_list, skip_list, delimit_regx, delimit, parse_all, outmap, force)
        logging.info("Total sections: " + str(section_count) + "outmap sec: " + str(len(outmap)))
    else:
        logging.info("Old collectinfo version: " + filepath)
        extract_section_from_old_cinfo(filepath, filter_list, skip_list, non_delimit_regx, outmap, force)

    return section_count


def validateSectionCount(section_count, outmap, force):
    # Validate no of section in outmap
    if section_count != 0:
        outmap_sections = 0
        for key in outmap:
            outmap_sections += len(outmap[key])

        logging.debug("outmap_sec: " + str(outmap_sections) + "section_count: " + str(section_count))
        if outmap_sections != section_count:
            logging.error("Something wrong, no of section in file and no of extracted are not matching")
            logging.error("outmap_sec: " + str(outmap_sections) + "section_count: " + str(section_count))
            if not force:
                raise Exception("Extracted section count is not matching with section count in file.")


# Extract sections from old collectinfo files
def extract_section_from_old_cinfo(cinfo_path, filter_list, skip_list, regex, outmap, force):
    logging.info("Processing old collectinfo: " + cinfo_path)
    new_cinfo = False
    # Check if cinfo file doesn't exist in given path
    if not os.path.exists(cinfo_path):
        logging.warning("collectinfo doesn't exist at Path: " + cinfo_path)
        return
    infile = cinfo_path
    with open(infile, 'r') as inf:
        # Check if it is not old version collectinfo
        # It looks more dependent, find other way

        datastr = []            # Value field of section
        filter_sec = ''

        fileline = ''
        while(True):
            try:
                fileline = inf.readline()
            except UnicodeDecodeError as e:
                logging.warning('Error at: ' + fileline)
                logging.warning(e)
                continue
            is_filter_line = False

            for index, filter_obj in enumerate(filter_list):

                # Check if this filter doesn't have regex of same version as collectinfo.
                if regex not in filter_obj:
                    continue

                # Check if its EOF or filter line
                # Update outmap
                # Break if its EOF
                if fileline == '' or re.search(filter_obj[regex], fileline):
                    if filter_sec != '':
                        updateMap(new_cinfo, filter_sec, datastr, outmap, skip_list, force)
                        datastr = []
                    if fileline == '':
                        break
                    filter_sec = filter_obj['section']
                    is_filter_line = True
                    break

            # Break if its EOF
            if fileline == '':
                break

            # iF given line is not a filter line, add it to datastr section
            # TODO: remove it.
            if is_filter_line is False:
                datastr.append(fileline)


# Count no of sections in new cinfo file
def section_count_fun(cinfo_path, delimiter):
    section_count = 0
    if not os.path.exists(cinfo_path):
        logging.warning("collectinfo doesn't exist at path: " + cinfo_path)
        return

    infile = cinfo_path
    with open(infile, 'r') as inf:
        while(True):
            fileline = ''
            try:
                fileline = inf.readline()
                if fileline == '':
                    break
            except UnicodeDecodeError as e:
                logging.warning('Error at: ' + fileline)
                logging.warning(e)
                continue

            if re.search(delimiter, fileline):
                section_count += 1
    return section_count


# Correct the logic that if next section starts before 2 lines and section not detected. it should throw error,
# Update section name everytime a delimiter line hits, or something else whatever could be done. fix logic
# Extract sections from new collectinfo files, having delimiter.
def extract_section_from_new_cinfo(cinfo_path, filter_list, skip_list, regex, delimiter, parse_all, outmap, force):
    logging.info("Processing new collectinfo: " + cinfo_path)
    new_cinfo = True
    parse_section = 0
    # Check if cinfo file doesn't exist in given path
    if not os.path.exists(cinfo_path):
        logging.warning("collectinfo doesn't exist at path: " + cinfo_path)
        return

    # TODO: Change it to take dest_dir param
    infile = cinfo_path
    # infoLines = 0
    with open(infile, 'r') as inf:

        # Check if it is not new version collectinfo
        if is_delimiter_collectinfo(cinfo_path, delimiter) is False:
            logging.debug("Not a new collectinfo version: " + infile)
            return

        # inside_section = False
        while(True):
            fileline = ''
            try:
                fileline = inf.readline()
                if fileline == '':
                    break
            except UnicodeDecodeError as e:
                logging.warning('Error at: ' + fileline)
                logging.warning(e)
                continue
            datastr = []            # Value field of section

            # Identifie 'ASCOLLECTINFO' section
            if re.search(delimiter, fileline):
                known = False           # True if its known section
                filter_sec = ''         # Filter section
                index = 0
                eof = False
                while(True):
                    section_line = ''
                    try:
                        section_line = inf.readline()
                    except UnicodeDecodeError as e:
                        logging.warning('Error at: ' + section_line)
                        logging.warning(e)
                        continue

                    # index = 0
                    # If new delimiter detected or EOF detected
                    # Update previous section
                    if section_line == '' or len(section_line) < 300:
                        if((section_line == '' or re.search(delimiter, section_line))):
                        
                            if parse_all or (filter_sec is not 'Unknown'):
                                updateMap(new_cinfo, filter_sec, datastr, outmap, skip_list, force)
                                parse_section += parse_section

                                # All section from filter_list is parsed and parse_all has been set false
                                # So exit.
                                if not parse_all and parse_section >= len(filter_list):
                                    eof = True
                                    break

                            datastr = []
                            filter_sec = 'Unknown'
                            index = 0
                            known = False

                            # If its EOF, break loop
                            if(section_line == ''):
                                eof = True
                                break
                            continue

                        # If its EOF and it doesn't belong to any section, break
                        if(section_line == ''):
                            eof = True
                            break

                        # Check for only two lines after delimiter for filter line
                        if index <= SECTION_DETECTION_LINE_MAX and known is False:
                            for f_index, filter_obj in enumerate(filter_list):

                                # Check if this filter doesn't have regex of same version as collectinfo.
                                if regex not in filter_obj:
                                    continue

                                if re.search(filter_obj[regex], section_line):
                                    known = True
                                    parse_section += parse_section
                                    filter_sec = filter_obj['section']
                                    break

                        # Append line in datastr
                    datastr.append(section_line)
                    index += 1

                    # IF after checking two lines, filter is not detected
                    # Its new section
                    # log warnings
                    # Raise exception if force option is not set.
                    if((index >= SECTION_DETECTION_LINE_MAX) and known is False) or\
                        (re.search(delimiter, section_line)):
                        logging.warning("Unknown section detected, printing first few lines:" + str(datastr[:3]))
                        if force is False:
                            raise Exception("Unknown section detected" + str(datastr[:3]))
                if(eof is True):
                    break



def extract_section_from_live_cmd(cmdName, cmdOutput, outmap):
    sectionName = ''
    for section in FILTER_LIST:
        if 'cmdName' in section and section['cmdName'] == cmdName:
            sectionName = section['section']
    if sectionName == '':
        logging.warning("Can not find section_name for cmdName: " + cmdName)
        return

    outmap[sectionName] = []
    outList = cmdOutput.split('\n')
    outmap[sectionName].append(outList)

    
# Cross_validate printconfig section in extracted section json from raw cinfo
def cross_validation_printconfig(cinfo_path):
    logging.info("Cross-validating printconfig")
    validator_list = ["microbenchmarks", "memory-accounting",
                      "paxos-max-cluster-size", "auto-dun", "fb-health-bad-pct"]
    # Some other uniq str
    # paxos-max-cluster-size
    # paxos-protocol
    # memory-accounting
    # fb-health-bad-pct

    validator_thr = MIN_VALIDATOR_STRING
    validator_section = 'printconfig'
    if not section_validator(validator_section, validator_list, validator_thr, cinfo_path):
        logging.warning("print config cross-validator failed. " + cinfo_path)
        raise Exception("print config cross-validator failed.")
    else:
        logging.info("print config cross-validator passed")


# Cross_validate stats section in extracted section json from raw cinfo
def cross_validation_statistics(cinfo_path):
    logging.info("Cross-validating statistics")
    validator_list = ["batch_errors", "batch_initiate", "err_write_fail_bin_exists"]
    # Some other uniq str
    # err_write_fail_generation
    # fabric_msgs_rcvd
    # partition_desync
    # proxy_initiate

    validator_thr = MIN_VALIDATOR_STRING
    validator_section = 'statistics'
    if not section_validator(validator_section, validator_list, validator_thr, cinfo_path):
        logging.warning("statistics cross-validator failed. " + cinfo_path)
        raise Exception("statistics cross-validator failed.")
    else:
        logging.info("statistics cross-validator passed")


# Cross_validate section in extracted section json from raw cinfo
def section_validator(validator_section, validator_list, validator_thr, cinfo_path):
    count = 0
    exist = False
    if not os.path.exists(cinfo_path):
        logging.warning("cinfo doesn't exist at path for validation: " + cinfo_path)
        return False
    if not cinfo_path.endswith(".json"):
        logging.warning("Not a cinfo file: " + cinfo_path)
        return True
    with open(cinfo_path) as cinfo_file:
        data = json.load(cinfo_file)

        # Skip files which are not valid cinfo
        if(len(data)) < MIN_SECTIONS_IN_COLLECT_INFO:
            return True

        if 'cinfo_paths' not in data.keys() or len(data['cinfo_paths']) == 0:
            logging.warning("cinfo doesn't have cinfo_paths.")
            return False

        logging.info(str(data['cinfo_paths']))
        for c_file in data['cinfo_paths']:
            with open(c_file, 'rb') as inf:
                for validator in validator_list:
                    # Set iterator to start of the file
                    inf.seek(0, 0)
                    for fileline in inf:
                        line = str(fileline)
                        if re.search(validator, line):
                            count += 1
                            # print("validator: " + validator + " line: " + line + " thr: " + str(validator_thr) + " count: " + str(count))
                            break
                    if count >= validator_thr:
                        exist = True
                        break

    # TODO: check these conditions
    # if validator_section in data.keys() and exist == True:
    #   return True
    # elif validator_section not in data.keys() and exist == False:
    #   return True
    # else:
    #   return False
    if(validator_section not in data.keys() and exist is True):
        return False
    else:
        return True


