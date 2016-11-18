import section_parser
import cinfo_parser
import section_filter_list
import logging

SECTION_NAME_LIST = section_filter_list.SECTION_NAME_LIST

def parseAllStatsCinfo(filepath, parsedOutput, force = False):
	parseAllAsStatsCinfo(filepath, parsedOutput, force)
	parseAllSysStatsCinfo(filepath, parsedOutput, force)


def parseAllAsStatsCinfo(filepath, parsedOutput, force = False):
	# Parse collectinfo and create intermediate section_map
	logging.info("Parsing All aerospike stat sections.")
	outmap = {}
	cinfo_parser.extract_validate_filter_section_from_file(filepath, outmap, force)

	section_parser.parseAsSection(SECTION_NAME_LIST, outmap, parsedOutput)



def parseAllSysStatsCinfo(filepath, parsedOutput, force = False):
	# Parse collectinfo and create intermediate section_map
	logging.info("Parsing All sys stat sections.")
	outmap = {}
	cinfo_parser.extract_validate_filter_section_from_file(filepath, outmap, force)

	section_parser.parseSysSection(SECTION_NAME_LIST, outmap, parsedOutput)



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
	extract_section_from_live_cmd(cmdName, cmdOutput, outmap)

	section_parser.parseSysSection(sectionList, outmap, parsedOutput)

