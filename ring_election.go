// Ring election

// Defined rule: The leader is always the process with the greatest ID.

package main

import (
	"fmt"
	"sync"
)

type mensagem struct {
	tipo         int
	corpo        [4]int
	actualLeader int
	startPoint   int
}

var (
	chans = []chan mensagem{
		make(chan mensagem),
		make(chan mensagem),
		make(chan mensagem),
		make(chan mensagem),
	}
	controle = make(chan int)
	wg       sync.WaitGroup
)

func ControlerOrder(temp mensagem, tipoMsg int, nProcess, nCanal int, in chan int) {
	temp.tipo = tipoMsg
	chans[nCanal] <- temp

	if tipoMsg == 2 {
		fmt.Printf("Controle: mudar o processo %d para falho\n", nProcess)
		fmt.Printf("Controle: confirmação %d\n", <-in)
	}
	if tipoMsg == 3 {
		fmt.Printf("Controle: mudar o processo %d para ativo\n", nProcess)
		fmt.Printf("Controle: confirmação %d\n", <-in)
	}
}

func ElectionControler(in chan int) {
	defer wg.Done()

	var temp mensagem

	// SIMULAÇÃO

	// líder inicial é o processo 3 (maior ID entre todos processos)
	temp.actualLeader = 3

	// mudar o processo 3 - canal de entrada 2 - para falho
	ControlerOrder(temp, 2, 3, 2, in)

	// processo 0 dispara eleição
	ControlerOrder(temp, 1, 0, 3, in)

	// controle espera pelo resultado da eleição
	for {
		result := <-in
		fmt.Printf("Controle: resultado da eleição recebido. Novo líder é o processo %d.\n", result)
		temp.actualLeader = result
		break
	}

	// mudar o processo 3 - canal de entrada 2 - para ativo
	ControlerOrder(temp, 3, 3, 2, in)

	// processo 3 dispara eleição
	ControlerOrder(temp, 1, 3, 2, in)

	// controle espera pelo resultado da eleição
	for {
		result := <-in
		fmt.Printf("Controle: resultado da eleição recebido. Novo líder é o processo %d.\n", result)
		temp.actualLeader = result
		break
	}

	fmt.Println("\n   Processo controlador concluído.")
	fmt.Println("\n   Finalizando demais processos.\n")

	temp.tipo = 6
	chans[3] <- temp
	chans[2] <- temp
	chans[1] <- temp
	chans[0] <- temp
}

func ElectionStage(TaskId int, in chan mensagem, out chan mensagem, leader int) {
	defer wg.Done()

	var actualLeader = leader
	var bFailed = false

	for {
		temp := <-in

		switch temp.tipo {
		case 1:
			fmt.Printf("%2d: Iniciando eleição.\n", TaskId)
			temp.tipo = 4
			temp.corpo = [4]int{-1, -1, -1, -1}
			temp.corpo[TaskId] = TaskId
			temp.startPoint = TaskId
			fmt.Printf("%2d: voto registrado.\n", TaskId)
			fmt.Printf("%2d: enviando mensagem de votação (tipo 4) para o processo seguinte.\n", TaskId)
			out <- temp

		case 2:
			bFailed = true
			fmt.Printf("%2d: falho %v \n", TaskId, bFailed)
			fmt.Printf("%2d: lider atual %d\n", TaskId, actualLeader)
			controle <- -5

		case 3:
			bFailed = false

			if temp.actualLeader != actualLeader {
				actualLeader = temp.actualLeader
			}

			fmt.Printf("%2d: falho %v \n", TaskId, bFailed)
			fmt.Printf("%2d: lider atual %d\n", TaskId, actualLeader)
			controle <- -5

		case 4:
			if bFailed {
				fmt.Printf("%2d: (falho) repassando mensagem de votação (tipo %d).\n", TaskId, temp.tipo)
				out <- temp
				continue
			}

			// condição pra detectar quando fechar o ciclo da eleição
			if temp.startPoint == TaskId {
				maxID := -1
				for _, id := range temp.corpo {
					if id > maxID && id >= 0 {
						maxID = id
					}
				}
				fmt.Printf("%2d: Fim da eleição. Novo líder é o processo %d\n", TaskId, maxID)
				actualLeader = maxID

				// informa os demais processos do resultado da eleição
				fmt.Printf("%2d: enviando mensagem de resultado (tipo 5) para o processo seguinte.\n", TaskId)
				temp.tipo = 5
				temp.corpo = [4]int{-1, -1, -1, -1}
				temp.actualLeader = maxID
				out <- temp

			} else {
				temp.corpo[TaskId] = TaskId
				fmt.Printf("%2d: voto registrado.\n", TaskId)
				fmt.Printf("%2d: enviando mensagem de votação (tipo 4) para o processo seguinte.\n", TaskId)
				out <- temp
			}
		case 5:
			if temp.startPoint == TaskId {
				fmt.Printf("%2d: confirmo que todos os processos ativos do anel conhecem o novo líder. Finalizando eleição.\n", TaskId)
				temp.startPoint = -1
				fmt.Printf("%2d: informando controle sobre o resultado da eleição.\n", TaskId)
				controle <- temp.actualLeader
			} else {
				if !bFailed {
					fmt.Printf("%2d: recebendo mensagem de resultado (tipo 5). Novo líder é o processo %d\n", TaskId, temp.actualLeader)
					actualLeader = temp.actualLeader
					out <- temp
				} else {
					fmt.Printf("%2d: (falho) repassando mensagem de resultado (tipo %d).\n", TaskId, temp.tipo)
					out <- temp
					continue
				}
			}
		case 6:
			return
		default:
			fmt.Printf("%2d: não conheço este tipo de mensagem\n", TaskId)
			fmt.Printf("%2d: lider atual %d\n", TaskId, actualLeader)
		}
	}
}

func main() {

	wg.Add(5)

	// criar os processo do anel de eleicao

	go ElectionStage(0, chans[3], chans[0], 0) // este é o lider
	go ElectionStage(1, chans[0], chans[1], 0) // não é lider, é o processo 0
	go ElectionStage(2, chans[1], chans[2], 0) // não é lider, é o processo 0
	go ElectionStage(3, chans[2], chans[3], 0) // não é lider, é o processo 0

	fmt.Println("\n   Anel de processos criado")

	// criar o processo controlador

	go ElectionControler(controle)

	fmt.Println("\n   Processo controlador criado\n")

	wg.Wait() // Wait for the goroutines to finish
}
