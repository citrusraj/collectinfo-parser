#!/usr/local/bin/python3

#TODO: check how many are there
#obfuscated (/opt/aerospike/smd/UDF.smd.save)(/opt/aerospike/smd/UDF.smd)(/opt/aerospike/smd/sindex_module.smd.save)
#an empty section having just section name (netstat)

CMD_PREFIX = 'running shell command: '
# Section filter list.
# Param enable: Enable or disable dumping of section in parsed file.
# Param section: Section name.
# Param regex_new: regex for collectinfos having delimiter.
# Param regex_old: regex for collectinfos, not having delimiter.
FILTER_LIST = [
    {
        'enable': True,
        'section': 'Node',
        'regex_new': '^Node\n',
        'regex_old': '^Node\n'
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'Namespace',
        'regex_new': "^Namespace\n|\['namespace'\]",
        'regex_old': '^Namespace\n'
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'XDR',
        'regex_new': "\['xdr'\]|^XDR\n",
        'regex_old': '^XDR\n'
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'SETS',
        'regex_new': "^SETS\n|\['set'\]",
        'regex_old': '^SETS\n'
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'printconfig',
        'cmdName': 'config',
        'regex_new': "printconfig|\['config'\]",
        'regex_old': '^printconfig\n'
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'xdr_config',
        'regex_new': "\['config', 'xdr'\]"
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'dc_config',
        'regex_new': "\['config', 'dc'\]"
    },
    {
        'enable': True,
        'section': 'compareconfig',
        'regex_new': 'compareconfig',
        'regex_old': '^compareconfig\n'
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'diff config',
        'regex_new': "\['config', 'diff'\]",
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'latency',
        'cmdName': 'latency',
        'regex_new': 'latency',
        'regex_old': '^latency\n'
        # 'parser_func'
    },
    {
    #TODO:-----------
        'enable': True,
        'section': 'statistics',
        'cmdName': 'statistics',
        'regex_new': "^stat\n|\['statistics'\]|\"stat\"",
        'regex_old': '^stat\n'
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'statistics_xdr',
        'regex_new': "\['statistics', 'xdr'\]",
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'statistic_dc',
        'regex_new': "\['statistics', 'dc'\]",
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'statistic_sindex',
        'regex_new': "\['statistics', 'sindex'\]",
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'objsz',
        'regex_new': '^objsz\n|-v objsz',
        'regex_old': '^objsz\n'
        # 'parser_func'
    },
    {
    #TODO:--------
        'enable': True,
        'section': 'ttl-distribution_1',
        #'regex_new': 'ttl',
        #'regex_new': "[INFO] Data collection for ['distribution'] in progress..",
        'regex_new': "^ttl\n|-v ttl",
        'regex_old': '^ttl\n'
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'evict',
        'regex_new': '^evict\n|-v evict',
        'regex_old': '^evict\n'
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'NAMESPACE STATS',
        'regex_new': '^NAMESPACE STATS\n',
        'regex_old': '^NAMESPACE STATS\n'
        # 'parser_func'
    },

    {
        'enable': True, 
        'section': 'XDR STATS',
        'regex_new': '^XDR STATS\n',
        'regex_old': '^XDR STATS\n'
        # 'parser_func'
    },
    {
    #TODO:----------
        'enable': False,
        'section': "sudo lsof|grep `sudo ps aux|grep -v grep|grep -E 'asd|cld'|awk '{print $2}'`",
        'regex_new': "sudo lsof[|]grep `sudo ps aux[|]grep -v grep[|]grep -E 'asd[|]cld'[|]awk '[{]print [$]2[}]'`",
        'regex_old': CMD_PREFIX + "sudo lsof[|]grep `sudo ps aux[|]grep -v grep[|]grep -E 'asd[|]cld'[|]awk '[{]print [$]2[}]'`"
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'date',
        'regex_new': 'date',
        'regex_old': CMD_PREFIX + 'date'
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'hostname',
        'regex_new': 'hostname',
        'regex_old': CMD_PREFIX + 'hostname'
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'ifconfig',
        'regex_new': 'ifconfig',
        'regex_old': CMD_PREFIX + 'ifconfig'
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'uname -a',
        'cmdName': 'uname',
        'regex_new': 'uname -a',
        'regex_old': CMD_PREFIX + 'uname -a'
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'lsb_release_1',
        'cmdName': 'lsb',
        'regex_new': 'lsb_release -a',
        'regex_old': CMD_PREFIX + 'lsb_release -a'
        # 'parser_func'
    },
    # Two sections having lsb, they both could occure in file.
    {
        'enable': True,
        'section': 'lsb_release_2',
        'cmdName': 'lsb',
        'regex_new': 'ls /etc[|]grep release[|]xargs -I f cat /etc/f',
        'regex_old': CMD_PREFIX + 'ls /etc[|]grep release[|]xargs -I f cat /etc/f'
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'build rpm',
        'regex_new': 'rpm -qa[|]grep -E "citrus[|]aero"',
        'regex_old': CMD_PREFIX + 'rpm -qa[|]grep -E "citrus[|]aero"'
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'build dpkg',
        'regex_new': 'dpkg -l[|]grep -E "citrus[|]aero"',
        'regex_old': CMD_PREFIX + 'dpkg -l[|]grep -E "citrus[|]aero"'
        # 'parser_func'
    },
    {
        'enable': False,
        'section': 'aero_log',
        'regex_new': 'tail -n 10* .*aerospike.log',
        'regex_old': CMD_PREFIX + 'tail -n 10* .*aerospike.log'
        # 'parser_func'
    },
    {
        'enable': False,
        'section': 'citrus_log',
        'regex_new': 'tail -n 10* .*citrusleaf.log',
        'regex_old': CMD_PREFIX + 'tail -n 10* .*citrusleaf.log'
        # 'parser_func'
    },
    {
        'enable': False,
        'section': 'All aerospike/*.log',
        'regex_new': 'tail -n 10* .*aerospike/[*].log',
        'regex_old': CMD_PREFIX + 'tail -n 10* .*aerospike/[*].log',
        # 'parser_func'
    },
    {
        'enable': False,
        'section': 'Udf log',
        'regex_new': 'tail -n 10* .*aerospike/udf.log',
        'regex_old': CMD_PREFIX + 'tail -n 10* .*aerospike/*.log',
        # 'parser_func'
    },
    {
        'enable': False,
        'section': 'All citrusleaf/*.log',
        'regex_new': 'tail -n 10* .*citrusleaf/[*].log',
        'regex_old': CMD_PREFIX + 'tail -n 10* .*citrusleaf/[*].log',
        # 'parser_func'
    },
    {
        'enable': False,
        'section': 'xdr_log',
        'regex_new': 'tail -n 10* /var/log/.*xdr.log',
        'regex_old': CMD_PREFIX + 'tail -n 10* /var/log/.*xdr.log'
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'netstat -pant|grep 3000',
        'regex_new': 'netstat -pant[|]grep 3000|^netstat\n',
        'regex_old': CMD_PREFIX + 'netstat -pant[|]grep 3000'
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'top -n3 -b',
        'cmdName': 'top',
        'regex_new': 'top -n3 -b',
        'regex_old': CMD_PREFIX + 'top -n3 -b'
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'free -m',
        'cmdName': 'free',
        'regex_new': 'free -m',
        'regex_old': CMD_PREFIX + 'free -m'
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'df -h',
        'cmdName': 'df',
        'regex_new': 'df -h',
        'regex_old': CMD_PREFIX + 'df -h'
        # 'parser_func'
    },
    {
        'enable': True,
        #'section': 'ls /sys/block/{sd*,xvd*}/queue/rotational |xargs -I f sh -c "echo f; cat f',
        'section': 'rotational_disk_info',
        'regex_new': 'ls /sys/block/{sd[*],xvd[*]}/queue/rotational [|]xargs -I f sh -c "echo f; cat f;"',
        'regex_old': CMD_PREFIX + 'ls /sys/block/sd[*]/queue/rotational [|]xargs -I f sh -c "echo f; cat f;"'
        # 'parser_fun'
    },
    {
        'enable': True,
        'section': 'ls /sys/block/{sd*,xvd*}/device/model',
        'regex_new': 'ls /sys/block/{sd[*],xvd[*]}/device/model [|]xargs -I f sh -c "echo f; cat f;"',
        'regex_old': CMD_PREFIX + 'ls /sys/block/{sd[*],xvd[*]}/device/model [|]xargs -I f sh -c "echo f; cat f;"',
        # 'parser_func':
    },
    {
        'enable': False,
        'section': 'lsof',
        'regex_new': '(?=.*lsof)(?!.*grep)',
        'regex_old': CMD_PREFIX + '(?=.*lsof)(?!.*grep)'
        # 'parser_func':
    },
    {
        'enable': False,
        'section': 'dmesg',
        'regex_new': 'dmesg',
        'regex_old': CMD_PREFIX + 'dmesg'
        # 'parser_func':
    },
    {
        'enable': True,
        'section': 'iostat -x',
        'regex_new': 'iostat -x 1 10',
        'regex_old': CMD_PREFIX + 'iostat -x|iostat -x 1 10'
        # 'parser_func':
    },
    {
        'enable': True,
        'section': 'vmstat -s',
        'regex_new': 'vmstat -s',
        'regex_old': CMD_PREFIX + 'vmstat -s',
        # 'parser_func':
    },
    {
        'enable': True,
        'section': 'vmstat -m',
        'regex_new': 'vmstat -m',
        'regex_old': CMD_PREFIX + 'vmstat -m',
        # 'parser_func':
    },
    {
        'enable': True,
        'section': 'iptables -L',
        'regex_new': 'iptables -L',
        'regex_old': CMD_PREFIX + 'iptables -L',
        # 'parser_func':
    },
    {
        'enable': True,
        'section': 'aero_conf',
        'regex_new': 'cat /etc/aerospike/aerospike.conf',
        'regex_old': CMD_PREFIX + 'cat /etc/aerospike/aerospike.conf'
        # 'parser_func':
    },
    {
        'enable': True,
        'section': 'citrus_conf',
        'regex_new': 'cat /etc/citrusleaf/citrusleaf.conf',
        'regex_old': CMD_PREFIX + 'cat /etc/citrusleaf/citrusleaf.conf'
        # 'parser_func':
    },
    {
        'enable': True,
        'section': 'info_network',
        'regex_new': "'network'",
        # 'parser_func':
    },
    {
        'enable': True,
        'section': 'info_service table',
        #'regex_new': '(?=.*service)(?!.*services)',
        'regex_new': "'service'"
        # 'parser_func':
    },
    {
        'enable': True,
        'section': 'info_sindex',
        'cmdName': 'sindex_info',
        'regex_new': "\['sindex'\]",
        # 'parser_func':
    },
    # This is technically a ttl section, of different format
    {
        'enable': True,
        'section':'info_ttl_distribution_2',
        'regex_new': "\['distribution'\]",
        # 'parser_func':
    },
    {
        'enable': True,
        'section':'info_eviction_distribution_2',
        'regex_new': "\['distribution', 'eviction'\]",
        # 'parser_func':
    },
    {
        'enable': True,
        'section':'info_objectsz_distribution_2',
        'regex_new': "\['distribution', 'object_size', '-b'\]",
        # 'parser_func':
    },
    {
    #TODO:----------------
        'enable': True,
        'section':'info_service list',
        #'regex_new': '[INFO] Data collection for service in progress..',
        'regex_new': "(?=.*service)(?!.*services)"
        # 'parser_func':
    },
    {
        'enable': True,
        'section':'info_services',
        'regex_new': 'services'
        # 'parser_func':
    },
    {
        'enable': True,
        'section':'info_xdr-min-lastshipinfo',
        'regex_new': 'xdr-min-lastshipinfo:',
        # 'parser_func':
    },
    {
        'enable': True,
        'section':'info_dump-fabric',
        'regex_new': 'dump-fabric:',
        # 'parser_func':
    },
    {
        'enable': True,
        'section':'info_dump-hb:',
        'regex_new': 'dump-hb:',
        # 'parser_func':
    },
    {
        'enable': True,
        'section':'info_dump-migrates:',
        'regex_new': 'dump-migrates:',
        # 'parser_func':
    },
    {
        'enable': True,
        'section':'info_dump-msgs:',
        'regex_new': 'dump-msgs:',
        # 'parser_func':
    },
    {
        'enable': True,
        'section': 'info_dump-paxos:',
        'regex_new': 'dump-paxos:',
        # 'parser_func':
    },
    {
        'enable': True,
        'section': 'info_dump-smd:',
        'regex_new': 'dump-smd:',
        # 'parser_func':
    },
    {
        'enable': True,
        'section': 'info_dump-wb:',
        'regex_new': 'dump-wb:',
        # 'parser_func':
    },
    {
        'enable': True,
        'section': 'info_infodump-wb-summary',
        'regex_new': 'dump-wb-summary:',
        # 'parser_func':
    },
    {
        'enable': True,
        'section': 'info_dump-wr',
        'regex_new': 'dump-wr:',
        # 'parser_func':
    },
    {
        'enable': True,
        'section': 'info_sindex-dump:',
        'regex_new': 'sindex-dump:',
        # 'parser_func':
    },
    {
        'enable': True,
        'section': 'info_uptime',
        'regex_new': 'uptime',
        # 'parser_func':
    },
    {
        'enable': True,
        'section': 'info_collect_sys',
        'regex_new': 'collect_sys',
        # 'parser_func':
    },
    {
        'enable': True,
        'section': 'info_get_awsdata',
        'cmdName': 'awsdata',
        'regex_new': 'get_awsdata',
        # 'parser_func':
    },
    {
        'enable': True,
        'section': 'info_stderr',
        'regex_new': 'tail -n 10* stderr'
        # 'parser_func':
    },
    {
        'enable': True,
        'section': 'info_ip addr',
        'regex_new': 'ip addr'
        # 'parser_func':
    },
    {
        'enable': True,
        'section': 'info_ip_link',
        'regex_new': 'ip -s link',
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'ss -pant',
        'regex_new': "\['ss -pant'\]",
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'ss -pant | grep .* | grep TIME-WAIT | wc -l',
        'regex_new': 'ss -pant [|] grep .* [|] grep TIME-WAIT [|] wc -l',
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'ss -pant | grep .* | grep CLOSE-WAIT | wc -l',
        'regex_new': 'ss -pant [|] grep .* [|] grep CLOSE-WAIT [|] wc -l',
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'ss -pant | grep .* | grep ESTAB | wc -l',
        'regex_new': 'ss -pant [|] grep .* [|] grep ESTAB [|] wc -l',
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'sar -n EDEV',
        'regex_new': 'sar -n EDEV',
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'sar -n DEV',
        'regex_new': 'sar -n DEV'
    },
    {
        'enable': False,
        'section': 'obfuscated',
        'regex_new': 'obfuscated',
        # 'parser_func'
    },
    {
        'enable': False,
        'section': 'aerospike_critical.log',
        'regex_new': 'tail -n 10* .*aerospike/aerospike_critical.log',
        # 'parser_func'
    },
    {
        'enable': False,
        'section': 'log messages',
        'regex_new': 'cat /var/log/messages',
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'Running with Force on Offline Aerospike Server',
        'regex_new': 'Running with Force on Offline Aerospike Server',
        'regex_old': 'Running with Force on Offline Aerospike Server',
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'sysctl',
        'regex_new': 'sudo sysctl -a [|] grep -E "shmmax[|]file-max[|]maxfiles"',
        # 'parser_func'
    },
    {
        # Its AWS info
        'enable': True,
        'section': 'Request metadata',
        'regex_new': 'Requesting... http://',
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'DC info',
        'regex_new': "'dc'",
        # 'parser_func'
    },
    {
        'enable': True,
        'section': "features'",
        'regex_new': "'features'",
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'mpstat -P ALL 2 3',
        'regex_new': 'mpstat -P ALL 2 3',
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'cpuinfo',
        'regex_new': "\['cpuinfo'\]|^cat /proc/cpuinfo\n"
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'ASD stats',
        'regex_new': "^ASD STATS\n"
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'aerospike profiling conf',
        'regex_new': "cat /etc/aerospike/aerospike_profiling.conf"
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'meminfo',
        'cmdName': 'meminfo',
        'regex_new': "cat /proc/meminfo"
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'interrupts',
        'regex_new': "cat /proc/interrupts"
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'asadm version',
        'regex_new': "asadm version"
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'pmap',
        'regex_new': "\['pmap'\]"
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'syslog',
        'regex_new': "cat /var/log/syslog"
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'partition-info',
        'regex_new': "partition-info"
        # 'parser_func'
    },
    {
        'enable': True,
        'section': 'hist-dump:ttl',
        'regex_new': "hist-dump:ns=.*;hist=ttl"
    # 'parser_func'
    },
    {
        'enable': True,
        'section': 'hist-dump:objsz',
        'regex_new': "hist-dump:ns=.*;hist=objsz"
    # 'parser_func'
    }
#{
#    'enable': True,
#    'section': 'set',
#    'regex_new': "\['set'\]"
#    # 'parser_func'
#}
]

SKIP_LIST = ['hist-dump', 'dump-wb-summary']
SECTION_NAME_LIST = ['statistics', 'config', 'latency', 'sindex_info', 'top', 'lsb', 'uname', 'meminfo', 'awsdata']
