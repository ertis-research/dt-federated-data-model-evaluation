//go:build linux

package main

import (
	"syscall"
	"time"
)

// getCPUTimeMs devuelve el tiempo real de procesador consumido (en milisegundos).
func getCPUTimeMs() float64 {
	var ru syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	cpuTime := time.Duration(ru.Utime.Sec)*time.Second + time.Duration(ru.Utime.Usec)*time.Microsecond +
		time.Duration(ru.Stime.Sec)*time.Second + time.Duration(ru.Stime.Usec)*time.Microsecond
	return float64(cpuTime.Microseconds()) / 1000.0
}
