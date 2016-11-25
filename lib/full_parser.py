import section_parser
import cinfo_parser
import section_filter_list
import logging

SECTION_NAME_LIST = section_filter_list.SECTION_NAME_LIST
SECTION_FILTER_LIST = section_filter_list.FILTER_LIST

def parseAllStatsCinfo(filepath, parsedOutput, force = False):
    parseAllAsStatsCinfo(filepath, parsedOutput, force)
    parseAllSysStatsCinfo(filepath, parsedOutput, force)


def parseAllAsStatsCinfo(filepath, parsedOutput, force = False):
    # Parse collectinfo and create intermediate section_map
    logging.info("Parsing All aerospike stat sections.")
    outmap = {}
    outmap_section_list = []
    cinfo_parser.extract_validate_filter_section_from_file(filepath, outmap, force)
    for section_id in outmap['section_ids']:
        if 'final_section_name' in SECTION_FILTER_LIST[section_id]:
            outmap_section_list.append(SECTION_FILTER_LIST[section_id]['final_section_name'])

    sction_filter_list = list(set(outmap_section_list).intersection(SECTION_NAME_LIST))

    logging.info("Parsing sections: " + str(sction_filter_list))
    section_parser.parseAsSection(sction_filter_list, outmap, parsedOutput)



def parseAllSysStatsCinfo(filepath, parsedOutput, force = False):
    # Parse collectinfo and create intermediate section_map
    logging.info("Parsing All sys stat sections.")
    outmap = {}
    outmap_section_list = []
    cinfo_parser.extract_validate_filter_section_from_file(filepath, outmap, force)
    for section_id in outmap['section_ids']:
        if 'final_section_name' in SECTION_FILTER_LIST[section_id]:
            outmap_section_list.append(SECTION_FILTER_LIST[section_id]['final_section_name'])

    sction_filter_list = list(set(outmap_section_list).intersection(SECTION_NAME_LIST))


    logging.info("Parsing sections: " + str(sction_filter_list))
    section_parser.parseSysSection(sction_filter_list, outmap, parsedOutput)



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

