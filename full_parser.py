import section_parser
import cinfo_parser
import section_filter_list
import logging
SECTION_FILTER_LIST = section_filter_list.FILTER_LIST
SKIP_LIST = section_filter_list.SKIP_LIST


def parseAllStatsCinfo(filepath, parsedOutput, force = False):
	parseAllAsStatsCinfo(filepath, parsedOutput, force)
	parseAllSysStatsCinfo(filepath, parsedOutput, force)


def parseAllAsStatsCinfo(filepath, parsedOutput, force = False):
	# Parse collectinfo and create intermediate section_map
	logging.info("Parsing All aerospike stat sections.")
	outmap = {}
	parse_all = True
	skip_list = SKIP_LIST
	filter_list = SECTION_FILTER_LIST
	
	logging.info("Creating section.json")
	cinfo_parser.extract_section_from_file(filepath, filter_list, skip_list, parse_all, outmap, force)
	cinfo_parser.filter_processed_cinfo(outmap, filter_list)


	# Parse all Aerospike statistics stat sections
	logging.info("Creating parsed.json")
	nodes = section_parser.identifyNodes(outmap)
	if nodes:
		section_parser.parseConfigSection(nodes, outmap, parsedOutput)
		section_parser.parseStatSection(nodes, outmap, parsedOutput)
		section_parser.parseLatencySection(nodes, outmap, parsedOutput)
		section_parser.parseSindexInfoSection(nodes, outmap, parsedOutput)

	logging.info("Converting basic raw string vals to original vals.")
	section_parser.typeCheckBasicValues(parsedOutput)


def parseAllSysStatsCinfo(filepath, parsedOutput, force = False):
	# Parse collectinfo and create intermediate section_map
	logging.info("Parsing All system stat sections.")
	outmap = {}
	parse_all = True
	skip_list = SKIP_LIST
	filter_list = SECTION_FILTER_LIST
	
	logging.info("Creating section.json")
	cinfo_parser.extract_section_from_file(filepath, filter_list, skip_list, parse_all, outmap, force)
	cinfo_parser.filter_processed_cinfo(outmap, filter_list)


	# Parse all System stat
	logging.info("Creating parsed.json")
	section_parser.parseAWSDataSection(outmap, parsedOutput)
	section_parser.parseLSBReleaseSection(outmap, parsedOutput)
	section_parser.parseTopSection(outmap, parsedOutput)
	section_parser.parseUnameSection(outmap, parsedOutput)
	section_parser.parseMeminfoSection(outmap, parsedOutput)

	logging.info("Converting basic raw string vals to original vals.")
	section_parser.typeCheckBasicValues(parsedOutput)


def parseAsStatsCinfo(filepath, parsedOutput, section_list, force = False):
	# Parse collectinfo and create intermediate section_map
	logging.info("Parsing system stat sections.")
	outmap = {}
	parse_all =True
	skip_list = SKIP_LIST
	filter_list = SECTION_FILTER_LIST

	logging.info("Creating section.json")
	cinfo_parser.extract_section_from_file(filepath, filter_list, skip_list, parse_all, outmap, force)
	cinfo_parser.filter_processed_cinfo(outmap, filter_list)


	# Parse System stat
	nodes = section_parser.identifyNodes(outmap)
	if not nodes:
		logging.warning("Node can't be identified. Can not parse")
		return

	for section in section_list:
		logging.info("Parsing section: " + section)

		if section == 'statistics':
			section_parser.parseStatSection(nodes, outmap, parsedOutput)

		elif section == 'config':
			section_parser.parseConfigSection(nodes, outmap, parsedOutput)

		elif section == 'latency':
			section_parser.parseLatencySection(nodes, outmap, parsedOutput)

		elif section == 'sindex_info':
			section_parser.parseSindexInfoSection(nodes, outmap, parsedOutput)

		else:
			logging.warning("Section unknown, can not be parsed. Check SECTION_NAME_LIST.")

	logging.info("Converting basic raw string vals to original vals.")
	section_parser.typeCheckBasicValues(parsedOutput)



def parseSysStatsCinfo(filepath, parsedOutput, section_list, force = False):
	# Parse collectinfo and create intermediate section_map
	logging.info("Parsing system stat sections.")
	outmap = {}
	parse_all = True
	skip_list = SKIP_LIST
	filter_list = SECTION_FILTER_LIST
	
	logging.info("Creating section.json")
	cinfo_parser.extract_section_from_file(filepath, filter_list, skip_list, parse_all, outmap, force)
	cinfo_parser.filter_processed_cinfo(outmap, filter_list)


	# Parse System stat
	for section in section_list:
		logging.info("Parsing section: " + section)

		if section == 'top':
			section_parser.parseTopSection(outmap, parsedOutput)

		elif section == 'lsb':
			section_parser.parseLSBReleaseSection(outmap, parsedOutput)

		elif section == 'uname':
			section_parser.parseUnameSection(outmap, parsedOutput)

		elif section == 'meminfo':
			section_parser.parseMeminfoSection(outmap, parsedOutput)

		elif section == 'awsdata':
			section_parser.parseAWSDataSection(outmap, parsedOutput)
		else:
			logging.warning("Section unknown, can not be parsed. Check SECTION_NAME_LIST.")

	logging.info("Converting basic raw string vals to original vals.")
	section_parser.typeCheckBasicValues(parsedOutput)



