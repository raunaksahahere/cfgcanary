package main

import "os"

func main() {
	chunk := make([]byte, 4096)
	for i := range chunk {
		chunk[i] = 'x'
	}
	for i := 0; i < 1024; i++ {
		if _, err := os.Stdout.Write(chunk); err != nil {
			os.Exit(3)
		}
	}
}
