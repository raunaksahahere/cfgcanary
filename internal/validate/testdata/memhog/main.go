package main

import "fmt"

func main() {
	var keep [][]byte
	for i := 0; i < 4096; i++ {
		b := make([]byte, 1<<20)
		for j := range b {
			b[j] = byte(j)
		}
		keep = append(keep, b)
	}
	fmt.Println(`{"ok": true, "errors": []}`, len(keep))
}
