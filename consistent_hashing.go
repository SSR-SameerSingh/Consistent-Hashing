package main

import (
	"fmt"
	"sync"
	"time"
	"math/rand"
)

var ring = []int{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}
var wg sync.WaitGroup

func initialize_ring(server_list []int, chans []chan int) {
	j := 0
	allocate_points(server_list, chans, j) //allocate points

	for i := 1; i < len(server_list)+1; i ++ {
		virtualize(server_list, chans, i-1) // replicate server i
	}
}

func allocate_points(server_list []int, chans []chan int, start int) {
	for i := 0; i < len(server_list); i++ {
		idx := start % len(ring)
		if ring[idx] == 0 {
			ring[idx] = server_list[i]
			start += int(len(ring) / len(server_list))
		}
	}
}

func virtualize(server_list []int, chans []chan int, idx int) {
	server_name := server_list[idx]
	index := idx+1
	for i:= idx+1; i < len(server_list)+idx+1; i++ {
		index = index % len(ring)
		if ring[index] == 0 {
			ring[index] = server_name
		}
		index += int(len(ring) / len(server_list))
	}
}

func start_servers(server_list []int, chans []chan int) {
	for i,server := range(server_list) {
		fmt.Println("Starting server no:",server,"...")
		time.Sleep(time.Second * 1)
		wg.Add(1)
		go start(server, chans[i])
		fmt.Println("Server ",server, " started sucessfully.")
	}
}


func start(server int, c chan int) {
	// defer close(c)
	time.Sleep(time.Second * 1)
	for i := 0; i < 50; i ++{
		select {
		case req_id := <-c:
			fmt.Printf("Received request_id %d on server %d, sending response...\n", req_id, server)
			time.Sleep(time.Second * 1)
			fmt.Printf("Response sent by server %d for request_id %d \n", server, req_id)
		default:
			fmt.Println("No request to process, sitting idle. ;-P", server)
			time.Sleep(time.Second * 1)
		}
	}
	wg.Done()
	close(c)
	fmt.Printf("-*-*-*-*-*-*-*-*-*-*Closing server %d. Sayonara !!-*-*-*-*-*-*-*-*-*-*\n", server)
}

func serve_request(request_id int, loc int, server_list []int, chans []chan int) {
	fmt.Println("Received request_id:", request_id)
	generate_response(request_id, loc, server_list, chans)
}

func generate_response(request_id int, loc int, server_list []int, chans []chan int) {
	fmt.Println("going into generate response")
	if ring[loc] != 0 {
		server := ring[loc] // server which serves the request
		channel := chans[server-1] // message will be sent on this channel
		channel <- request_id // sending request id on assigned channel
	} else if ring[loc] == 0 {
		for {
			if ring[loc] != 0 {
				break
			}
			loc = (loc+1)%(len(ring))
		}
		server := ring[loc]
		channel := chans[server-1]
		channel <- request_id
	}
}

func send_requests(server_list []int, chans []chan int, requests []int) {
	s := rand.NewSource(time.Now().Unix())
	r := rand.New(s)
	r.Intn(len(requests))

	for i := 0; i < len(requests); i ++ {
		index := r.Intn(len(requests))
		request_id := requests[index]
		loc := index % len(ring)// hash request to get location on ring
		serve_request(request_id, loc, server_list, chans)
		time.Sleep(time.Second * 1)
	}
}

func main() {
	c1, c2, c3, c4 := make(chan int, 10), make(chan int, 10), make(chan int, 10), make(chan int, 10)
	chans := []chan int {c1, c2, c3, c4}

	server_list := []int {1,2,3,4}

	requests := []int {1,2,44,3,1,123,432,241,443,645,765,876,76,42,135,2,1,132,435,643,657,873,321,945,821,904, 87764}

	initialize_ring(server_list, chans)

	start_servers(server_list, chans)

	send_requests(server_list, chans, requests)

	wg.Wait()
	
	fmt.Println(ring)
	
}