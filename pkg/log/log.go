package log

import (
	"errors"
	"io"
	"os"
	"pg_lineage/pkg/config"

	"github.com/natefinch/lumberjack"
	"github.com/sirupsen/logrus"
)

type Logger interface {
	Fatalf(string, ...any)
	Errorf(string, ...any)
	Warningf(string, ...any)
	Warnf(string, ...any)
	Infof(string, ...any)
	Debugf(string, ...any)
	Fatal(...any)
	Error(...any)
	Warn(...any)
	Info(...any)
	Debug(...any)
}

var logger Logger

func GetLogger() Logger {
	return logger
}

func Fatalf(format string, args ...any) {
	logger.Fatalf(format, args...)
}

func Errorf(format string, args ...any) {
	logger.Errorf(format, args...)
}

func Warningf(format string, args ...any) {
	logger.Warningf(format, args...)
}

func Warnf(format string, args ...any) {
	logger.Warnf(format, args...)
}

func Infof(format string, args ...any) {
	logger.Infof(format, args...)
}

func Debugf(format string, args ...any) {
	logger.Debugf(format, args...)
}

func Fatal(args ...any) {
	logger.Fatal(args...)
}

func Error(args ...any) {
	logger.Error(args...)
}

func Warn(args ...any) {
	logger.Warn(args...)
}

func Info(args ...any) {
	logger.Info(args...)
}

func Debug(args ...any) {
	logger.Debug(args...)
}

func NewLogger(cfg *config.LogConfig) (Logger, error) {

	logrusLogger := logrus.New()

	// 设置日志格式为json格式
	logrusLogger.SetFormatter(&logrus.TextFormatter{})

	// 设置将日志输出到标准输出（默认的输出为stderr，标准错误）
	if cfg.Path == "" {
		return nil, errors.New("no log path")
	}
	lumberjackLogrotate := &lumberjack.Logger{
		Filename:   cfg.Path,
		MaxSize:    10, // Max megabytes before log is rotated
		MaxBackups: 30, // Max number of old log files to keep
		MaxAge:     30, // Max number of days to retain log files
		Compress:   true,
	}
	logrusLogger.SetOutput(io.MultiWriter(os.Stdout, lumberjackLogrotate))

	// 设置日志级别
	if cfg.Level == "" {
		cfg.Level = "info"
	}
	logLevel, err := logrus.ParseLevel(cfg.Level)
	if err != nil {
		logLevel = logrus.InfoLevel
	}
	logrusLogger.SetLevel(logLevel)

	return logrusLogger, nil
}

func InitLogger(cfg *config.LogConfig) error {
	var err error
	logger, err = NewLogger(cfg)
	return err
}
