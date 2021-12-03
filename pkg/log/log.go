package log

import (
	"errors"
	"io"
	"os"

	"github.com/natefinch/lumberjack"
	"github.com/sirupsen/logrus"
)

type Logger interface {
	Fatalf(string, ...interface{})
	Errorf(string, ...interface{})
	Warningf(string, ...interface{})
	Warnf(string, ...interface{})
	Infof(string, ...interface{})
	Debugf(string, ...interface{})
	Fatal(...interface{})
	Error(...interface{})
	Warn(...interface{})
	Info(...interface{})
	Debug(...interface{})
}

type LoggerConfig struct {
	Path  string `mapstructure:"path"`
	Level string `mapstructure:"level"`
}

var logger Logger

func GetLogger() Logger {
	return logger
}

func Fatalf(format string, args ...interface{}) {
	logger.Fatalf(format, args...)
}

func Errorf(format string, args ...interface{}) {
	logger.Errorf(format, args...)
}

func Warningf(format string, args ...interface{}) {
	logger.Warningf(format, args...)
}

func Warnf(format string, args ...interface{}) {
	logger.Warnf(format, args...)
}

func Infof(format string, args ...interface{}) {
	logger.Infof(format, args...)
}

func Debugf(format string, args ...interface{}) {
	logger.Debugf(format, args...)
}

func Fatal(args ...interface{}) {
	logger.Fatal(args...)
}

func Error(args ...interface{}) {
	logger.Error(args...)
}

func Warn(args ...interface{}) {
	logger.Warn(args...)
}

func Info(args ...interface{}) {
	logger.Info(args...)
}

func Debug(args ...interface{}) {
	logger.Debug(args...)
}

func NewLogger(cfg *LoggerConfig) (Logger, error) {

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

func InitLogger(cfg *LoggerConfig) error {
	var err error
	logger, err = NewLogger(cfg)
	return err
}
