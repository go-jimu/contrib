package zap

import (
	"fmt"

	"github.com/go-jimu/components/logger"
	"go.uber.org/zap"
)

type (
	ZapLogger struct {
		logger *zap.Logger
	}
)

var _ logger.Logger = (*ZapLogger)(nil)

func NewLog(zl *zap.Logger) logger.Logger {
	return &ZapLogger{logger: zl}
}

func (zl *ZapLogger) Log(level logger.Level, kvs ...interface{}) {
	if len(kvs) == 0 || len(kvs)&1 != 0 {
		zl.logger.Warn("", zap.Error(fmt.Errorf("kvs must appear in paris: %v", kvs)))
		return
	}

	var fields = make([]zap.Field, 0, len(kvs)%2)
	for i := 0; i < len(kvs); i += 2 {
		fields = append(fields, zap.Any(fmt.Sprint(kvs[i]), kvs[i+1]))
	}

	switch level {
	case logger.Debug:
		zl.logger.Debug("", fields...)
	case logger.Info:
		zl.logger.Info("", fields...)
	case logger.Warn:
		zl.logger.Warn("", fields...)
	case logger.Error:
		zl.logger.Error("", fields...)
	case logger.Panic:
		zl.logger.Panic("", fields...)
	case logger.Fatal:
		zl.logger.Fatal("", fields...)
	}
}
