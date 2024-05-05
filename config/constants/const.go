package constants

const LogNewLineDelim = "#|#"
const LogKeyValDelim = "@|@"
const LogFileMaxSizeBytes = 500
const LogFileNameFormat = "data_%s.log"
const DataSegmentMetaDataByteSize = 100

const ErrMsgNotFound = "NOT FOUND"
const ErrMsgInvalidInput = "invalid input"
const ErrMsgProtectedKeyword = "cannot use '%s' since it is a protected keyword"

const ClientTypeShell = "SHELL"
const ClientTypeTcp = "TCP"

const IndexTypeHashIndex = "HASH_INDEX"
const IndexTypeLSMIndex = "LSM_INDEX"

const LogDirectory = "/Users/manthanchauhan/GolandProjects/bitcask/log"

const DataDirectory = "/Users/manthanchauhan/GolandProjects/bitcask/dataLogs"
const DataDirectoryTest = DataDirectory + "/testDataLogs"

const DataDirectoryLSMIndex = DataDirectory + "/LsmIndexDataLogs"
const DataDirectoryLSMIndexTest = DataDirectoryTest + "/LsmIndexDataLogs"

const DataDirectoryHashIndex = DataDirectory + "/hashIndexDataLogs"
const DataDirectoryHashIndexTest = DataDirectoryTest + "/hashIndexDataLogs"

const SSTableBlockMaxKeys = 100
const MemTableMaxSizeBytes = 5000

const DeletedValuePlaceholder = "#!DEL!#"

var Keywords = []string{DeletedValuePlaceholder}
