//go:build windows

package main

// getCPUTimeMs devuelve 0 en Windows para evitar el error de compilación.
func getCPUTimeMs() float64 {
	return 0.0
}
