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
        if 'final_section_name' in SECTION_FILTER_LIST[section_id]:
            outmap_section_list.append(SECTION_FILTER_LIST[section_id]['final_section_name'])
    final_section_list = list(set(outmap_section_list).intersection(available_section))
    return final_section_list


def parseAllStatsCinfo(filepath, parsedOutput, force = False):
    parseAllAsStatsCinfo(filepath, parsedOutput, force)
    parseAllSysStatsCinfo(filepath, parsedOutput, force)


def parseAllAsStatsCinfo(filepath, parsedOutput, force = False):
    # Parse collectinfo and create intermediate section_map
    logging.info("Parsing All aerospike stat sections.")
    outmap = {}
    cinfo_parser.extract_validate_filter_section_from_file(filepath, outmap, force)

    section_filter_list = getSectionListForParsing(outmap, AS_SECTION_NAME_LIST)

    logging.info("Parsing sections: " + str(section_filter_list))
    section_parser.parseAsSection(section_filter_list, outmap, parsedOutput)



def parseAllSysStatsCinfo(filepath, parsedOutput, force = False):
    # Parse collectinfo and create intermediate section_map
    logging.info("Parsing All sys stat sections.")
    outmap = {}
    cinfo_parser.extract_validate_filter_section_from_file(filepath, outmap, force)
    section_filter_list = getSectionListForParsing(outmap, SYS_SECTION_NAME_LIST)

    logging.info("Parsing sections: " + str(section_filter_list))
    section_parser.parseSysSection(section_filter_list, outmap, parsedOutput)



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

