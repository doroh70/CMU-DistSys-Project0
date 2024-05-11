// Tests for SquarerImpl. Students should write their code in this file.

package p0partB

import (
	"fmt"
	"testing"
	"time"
)

const (
	timeoutMillis = 5000
)

func TestBasicCorrectness(t *testing.T) {
	fmt.Println("Running TestBasicCorrectness.")
	input := make(chan int)
	sq := SquarerImpl{}
	squares := sq.Initialize(input)
	go func() {
		input <- 2
	}()
	timeoutChan := time.After(time.Duration(timeoutMillis) * time.Millisecond)
	select {
	case <-timeoutChan:
		t.Error("Test timed out.")
	case result := <-squares:
		if result != 4 {
			t.Error("Error, got result", result, ", expected 4 (=2^2).")
		}
	}
}

func TestConcurrentInput(t *testing.T) {
	fmt.Println("Running TestConcurrentInput.")
	inputChan := make(chan int, 10)
	squarer := SquarerImpl{}
	outputChan := squarer.Initialize(inputChan)
	go func() {
		for i := 0; i < 5; i++ {
			inputChan <- i
		}
	}()
	go func() {
		for i := 5; i < 10; i++ {
			inputChan <- i
		}
	}()
	//Give routines time to finish
	time.Sleep(time.Duration(50) * time.Millisecond)
	close(inputChan)
	received := make(map[int]bool)
	for i := 0; i < 10; i++ {
		result := <-outputChan
		received[result] = true
	}
	for i := 0; i < 10; i++ {
		if _, ok := received[i*i]; !ok {
			t.Errorf("Missing square for %d", i)
		}
	}
}

func TestClose(t *testing.T) {
	fmt.Println("Running TestClose.")
	inputChan := make(chan int)
	sq := SquarerImpl{}
	squaresChan := sq.Initialize(inputChan)
	//Test that output channel is closed after Close() method is called
	sq.Close()
	timeoutChan := time.After(time.Duration(timeoutMillis) * time.Millisecond)
	//Let sender close input Chan
	close(inputChan)
	select {
	case _, ok2 := <-squaresChan:
		if ok2 {
			t.Error("Squares output channel not closed.")
		}
	case <-timeoutChan:
		t.Error("TestClose timed out.")
	}
}
