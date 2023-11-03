package main

import (
	"bufio"
	"log"
	"os"
	"pipeline/model"
	"strconv"
	"strings"
	"time"
)

const (
	bufferDrainInterval time.Duration = 10 * time.Second
	bufferSize          int           = 5
)

func main() {
	log.Println("Программа запущена")
	defer log.Println("Программа завершила работу")
	dataSource := func() (<-chan int, <-chan bool) {
		c := make(chan int)
		done := make(chan bool)
		go func() {
			defer close(done)
			scanner := bufio.NewScanner(os.Stdin)
			var data string
			for {
				scanner.Scan()
				data = scanner.Text()
				if strings.EqualFold(data, "exit") {
					return
				}
				i, err := strconv.Atoi(data)
				if err != nil {
					log.Println("Программа обрабатывает только целые числа!")
					continue
				}
				c <- i
			}
		}()
		return c, done
	}
	negativeFilterStageInt := func(done <-chan bool, c <-chan int) <-chan int {
		convertedIntChan := make(chan int)
		go func() {
			for {
				select {
				case data := <-c:
					if data > 0 {
						select {
						case convertedIntChan <- data:
							log.Println("Число прошло через negativeFilterStageInt")
						case <-done:
							log.Println("NegativeFilterStageInt завершила работу.")
							return
						}
					}
				case <-done:
					return
				}
			}
		}()
		return convertedIntChan
	}
	specialFilterStageInt := func(done <-chan bool, c <-chan int) <-chan int {
		filteredIntChan := make(chan int)
		go func() {
			for {
				select {
				case data := <-c:
					if data != 0 && data%3 == 0 {
						select {
						case filteredIntChan <- data:
							log.Println("Число прошло через specialFilterStageInt")
						case <-done:
							log.Println("SpecialFilterStageInt завершила работу.")
							return
						}
					}
				case <-done:
					return
				}
			}
		}()
		return filteredIntChan
	}
	bufferStageInt := func(done <-chan bool, c <-chan int) <-chan int {
		bufferedIntChan := make(chan int)
		buffer := model.NewRingIntBuffer(bufferSize)
		go func() {
			for {
				select {
				case data := <-c:
					buffer.Push(data)
					log.Println("Данные добавлены в буфер в bufferStageInt")
				case <-done:
					log.Println("BufferStageInt завершила работу.")
					return
				}
			}
		}()
		go func() {
			for {
				select {
				case <-time.After(bufferDrainInterval):
					bufferData := buffer.Get()
					if bufferData != nil {
						for _, data := range bufferData {
							select {
							case bufferedIntChan <- data:
								log.Println("Данные отправлены из буфера в bufferStageInt")
							case <-done:
								log.Println("BufferStageInt завершила работу.")
								return
							}
						}
					}
				case <-done:
					log.Println("BufferStageInt завершила работу.")
					return
				}
			}
		}()
		return bufferedIntChan
	}
	consumer := func(done <-chan bool, c <-chan int) {
		for {
			select {
			case data := <-c:
				log.Printf("Обработаны данные: %d\n", data)
			case <-done:
				log.Println("Consumer завершил работу.")
				return
			}
		}
	}

	log.Println("Запуск источника данных")
	source, done := dataSource()
	log.Println("Запуск пайплайна")
	pipeline := model.NewPipelineInt(done, negativeFilterStageInt, specialFilterStageInt, bufferStageInt)
	log.Println("Запуск потребителя")
	consumer(done, pipeline.Run(source))
}
