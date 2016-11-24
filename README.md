# Collectinfo and Live command parsing lib
This lib has parsing functions which can parse collectinfo file, its different sections  (which are aerospike stats and some system stats.) and multiple live system commands output. Parsed output will be a json map, which can be used as user wants.

There are two types of Parsing functions provides :

 - collectinfo parser funcions
 - live command parser functions


## collectinfo parser:
  These set of functions take 3 param as input, collectinfo filepath, parsedOutput which should be polulated, and force (In general if there are collisions in collectinfo sections or any unknown section than it throw exception. By this param that exception can be skipped.)

full_parser.py file has all parser wrapper functions.
 - parseAllAsStatsCinfo(filepath, parsedOutput, force = False)
 - parseAllSysStatsCinfo(filepath, parsedOutput, force = False)
 - parseAsStatsCinfo(filepath, parsedOutput, sectionList, force = False)
 - parseSysStatsCinfo(filepath, parsedOutput, sectionList, force = False)

These functions can be used to parse all sections for which parsers are provided, or a list of sections can be parsed.

## live command parser:
This function take 3 param. command_name (predefined fixed name for a command), command_output (output string of command output) and parsedOutput which should be polulated.

full_parser.py file has this parser wrapper functions.
 - parseSysStatsLiveCmd(cmdName, cmdOutput, parsedOutput)

## Section names to be parsed
### cmdName for sys commands:
 - top
 - lsb
 - uname
 - meminfo
 - awsdata
 - hostname
 - df
 - free-m
 - iostat
 - interrupts

**Note**- These cmdName can be used as a section name also for collectinfo parser func.
### Aerospike section name for parsing:
 - statistics
 - config
 - latency
 - sindex_info
                      
**Note**- These section names will be used in collectinfo parser function.

All details of these section names can be found in "section_filter_list.py" file.

                       

