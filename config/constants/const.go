package constants

const LogNewLineDelim = "#|#"
const LogKeyValDelim = "@|@"
const LogFileMaxSizeBytes = 500
const LogFileNameFormat = "data_%s.log"
const DataSegmentMetaDataByteSize = 100

const NotFoundMsg = "NOT FOUND"

const ClientTypeShell = "SHELL"
const ClientTypeTcp = "TCP"

const IndexTypeHashIndex = "HASH_INDEX"
const IndexTypeLSMIndex = "LSM_INDEX"

const DataDirectoryLSMIndex = "/Users/manthan/GolandProjects/bitcask/dataLogs/LsmIndexDataLogs/"

const SSTableBlockMaxKeys = 10
const MemTableMaxSizeBytes = 50
const MemTableWALDirectory = DataDirectoryLSMIndex + "WALs/"
