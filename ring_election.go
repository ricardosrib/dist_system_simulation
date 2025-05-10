// Ring election

// Defined rule: The leader is always the process with the smallest ID.

package main

import (
	"fmt"
	"sync"
)

type mensagem struct {
	tipo         int
	corpo        [4]int
	actualLeader int
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

func OrderControl(temp mensagem, tipoMsg int, canal int, in chan int) {
	temp.tipo = tipoMsg
	chans[canal] <- temp

	if tipoMsg == 2 {
		fmt.Printf("Controle: mudar o processo 0 para falho\n")
		fmt.Printf("Controle: confirmação %d\n", <-in)
	}
	if tipoMsg == 3 {
		fmt.Printf("Controle: mudar o processo 1 para ativo\n")
		fmt.Printf("Controle: confirmação %d\n", <-in)
	}
}

func ElectionControler(in chan int) {
	defer wg.Done()

	var temp mensagem

	// líder inicial é o processo 0
	temp.actualLeader = 0

	// mudar o processo 0 - canal de entrada 3 - para falho
	OrderControl(temp, 2, 3, in)

	// mudar o processo 1 - canal de entrada 0 - para falho
	OrderControl(temp, 2, 0, in)

	// matar os outros processos com mensagens não conhecidas (só pra consumir a leitura)
	// OrderControl(temp, 9, 1, in)
	// OrderControl(temp, 9, 2, in)

	// processo 2 dispara eleição
	OrderControl(temp, 1, 1, in)

	// controle espera pelo resultado da eleição
	for {
		result := <-in
		fmt.Printf("Controle: resultado da eleição recebido. Novo líder é o processo %d.\n", result)
		temp.actualLeader = result
		break
	}

	// mudar o processo 1 - canal de entrada 0 - para ativo
	OrderControl(temp, 3, 0, in)

	// processo 1 dispara eleição
	OrderControl(temp, 1, 0, in)

	// controle espera pelo resultado da eleição
	for {
		result := <-in
		fmt.Printf("Controle: resultado da eleição recebido. Novo líder é o processo %d.\n", result)
		temp.actualLeader = result
		break
	}

	// mudar o processo 0 - canal de entrada 3 - para ativo
	OrderControl(temp, 3, 3, in)

	// processo 0 dispara eleição
	OrderControl(temp, 1, 3, in)

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
			if temp.corpo[TaskId] != -1 {
				minID := 1000
				for _, id := range temp.corpo {
					if id < minID && id >= 0 {
						minID = id
					}
				}
				fmt.Printf("%2d: Fim da eleição. Novo líder é o processo %d\n", TaskId, minID)
				actualLeader = minID

				// informa os demais processos do resultado da eleição
				fmt.Printf("%2d: enviando mensagem de resultado (tipo 5) para o processo seguinte.\n", TaskId)
				temp.tipo = 5
				temp.corpo = [4]int{-1, -1, -1, -1}
				temp.actualLeader = minID
				out <- temp

			} else {
				temp.corpo[TaskId] = TaskId
				fmt.Printf("%2d: voto registrado.\n", TaskId)
				fmt.Printf("%2d: enviando mensagem de votação (tipo 4) para o processo seguinte.\n", TaskId)
				out <- temp
			}
		case 5:
			if TaskId == temp.actualLeader {
				fmt.Printf("%2d: confirmo que todos os processos ativos do anel conhecem o novo líder. Finalizando eleição.\n", TaskId)
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
