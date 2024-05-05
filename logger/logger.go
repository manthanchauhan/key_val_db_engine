package logger

import (
	"bitcask/config/constants"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var SugaredLogger *zap.SugaredLogger

func Init() {
	pe := zap.NewDevelopmentEncoderConfig()

	fileEncoder := zapcore.NewJSONEncoder(pe)

	pe.EncodeTime = zapcore.ISO8601TimeEncoder
	pe.EncodeCaller = zapcore.ShortCallerEncoder

	lumberJackLogger := getLumberJackLogger()

	core := zapcore.NewTee(
		zapcore.NewCore(fileEncoder, zapcore.AddSync(lumberJackLogger), zap.DebugLevel),
	)

	logger := zap.New(core)

	SugaredLogger = logger.Sugar()
}

func getLumberJackLogger() *lumberjack.Logger {
	return &lumberjack.Logger{
		Filename:   constants.LogDirectory + "/log.log",
		MaxSize:    2, // megabytes
		MaxBackups: 3, // number of log files
		MaxAge:     3, // days
	}
}

//func getLumberJackZapHook(logger *lumberjack.Logger) func(entry zapcore.Entry) error {
//	return func(e zapcore.Entry) error {
//		_, err := logger.Write([]byte(fmt.Sprintf("%+v", e)))
//
//		if err != nil {
//			return err
//		}
//
//		return nil
//	}
//}
