package main

import (
	"database/sql"

	_ "github.com/lib/pq"

	"log"
)

func main() {
	data := fetchData()

	currentState := make(map[string]string)
	edgeCount := make(map[edge]int)
	var states []string

	for _, v := range data {
		stateAlreadySeen := false
		for _, st := range states {
			if st == v.State {
				stateAlreadySeen = true
				break
			}
		}

		if !stateAlreadySeen {
			states = append(states, v.State)
		}

		if cur, ok := currentState[v.Id]; ok {
			// State transition
			ed := edge{
				StartState: cur,
				EndState:   v.State}

			if count, ok := edgeCount[ed]; ok {
				edgeCount[ed] = count + 1
			} else {
				edgeCount[ed] = 1
			}
		}
		currentState[v.Id] = v.State
	}

	log.Println("states found:")
	for _, st := range states {
		log.Println(st)
	}

	log.Println("edges found:")
	for k, v := range edgeCount {
		log.Printf("%v -> %v : %v", k.StartState, k.EndState, v)
	}

	log.Println("done")
}

type edge struct {
	StartState string
	EndState   string
}

type orderState struct {
	Id    string
	State string
}

func fetchData() (results []orderState) {
	log.Println("fetching data")
	defer log.Println("data fetched")

	db, err := sql.Open("postgres",
		"user=postgres dbname=cftool password=password")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	query := `select "OrdAccNo" as "Id","DataCode" as "State" from orders_log where "OrdAccNo" <> '' order by _seq_;`

	rows, err := db.Query(query)
	defer rows.Close()
	if err != nil {
		log.Fatal(err)
	}

	results = []orderState{}

	var (
		id    string
		state string
	)

	for rows.Next() {

		err = rows.Scan(&id, &state)
		if err != nil {
			return
		}

		results = append(results, orderState{
			Id:    id,
			State: state})
	}

	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}

	return
}
