package main

import (
	"code.google.com/p/gographviz"

	"flag"
	"fmt"

	"os"

	"io/ioutil"

	"strconv"

	"database/sql"

	_ "github.com/lib/pq"

	"log"
)

var stateNames = map[string]string{
	"A111": "New Order Acceptance",
	"B121": "Cancel Order Acceptance",
	"J211": "Execution Completion",
	"C119": "New Order Acceptance Error",
	"B131": "Modification Order Acceptance",
	"D129": "Cancel Order Acceptance Error",
	"A191": "Acceptance-related Notices Output Completion",
	"F231": "Modification Result",
	"D139": "Modification Order Acceptance Error",
	"K219": "New Order Registration Error",
	"K229": "Cancel Order Registration Error",
	"F221": "Cancel Result",
	"T321": "Price Limit Info",
	"K239": "Modification Order Registration Error",
	"K241": "Invalidation Result",
	"T111": "Market Administration",
	"T311": "Trading Halt Info",
	"T331": "Free Format",
	"6241": "Proxy Request",
	"6221": "Operation End",
	"J291": "Execution-related Notices Output",
	"6231": "Retransmission Request",
	"6261": "Proxy Status Enquiry",
	"6211": "Operation Start",
	"6271": "Order Sequence No. Enquiry",
	"6281": "Notice Sequence No. Enquiry",
	"6251": "Proxy Abort Request",
	"62A1": "Notice Destination Enquiry",
	"6291": "Notice Destination Setup Request",
	"T211": "Operation Start Normal Response",
	"T221": "Operation End Normal Response",
	"T229": "Operation End Error Response",
	"T231": "Retransimission Normal Response",
	"T219": "Operation Start Error Response",
	"T239": "Retransimission Error Response",
	"T241": "Proxy Normal Response",
	"T261": "Proxy Status Normal Response",
	"T249": "Proxy Error Response",
	"T259": "Proxy Abort Error Response",
	"T251": "Proxy Abort Normal Response",
	"T279": "Order Sequence No. Enquiry Error Response",
	"T269": "Proxy Status Error Response",
	"T271": "Order Sequence No. Enquiry Normal Response",
	"T291": "Notice Destination Setup Normal Response",
	"T299": "Notice Destination Setup Error Response",
	"T281": "Notice Sequence No. Enquiry Normal Response",
	"T289": "Notice Sequence No. Enquiry Error Response",
	"T2A1": "Notice Destination Enquiry Normal Response",
	"T2A9": "Notice Destination Enquiry Error Response",
	"T999": "Participant-Side-Error"}

type edge struct {
	StartState string
	EndState   string
}

type orderState struct {
	Id    string
	State string
}

var dbConnectionString = flag.String(
	"db",
	"",
	"Database connection string (postgres driver)")
var outFileName = flag.String("o", "out.dot", "Output file name")

func main() {
	flag.Parse()

	log.Println("Building .dot file for TSE state graph")
	defer log.Println("done")
	data := fetchData(*dbConnectionString)

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

	log.Println("edges found:")
	for k, v := range edgeCount {
		log.Printf("%v -> %v : %v", k.StartState, k.EndState, v)
	}

	graph := gographviz.NewGraph()
	graph.SetDir(true)
	graph.SetName("tsestates")
	graph.SetStrict(true)

	log.Println("states found:")
	for _, st := range states {
		graph.AddNode("", st, nil)
		log.Println(st)
	}

	log.Println("edges found:")
	for k, v := range edgeCount {
		log.Printf("%v -> %v : %v", k.StartState, k.EndState, v)
		graph.AddEdge(k.StartState, k.EndState, true, map[string]string{"label": strconv.Itoa(v)})
	}

	log.Printf("Writing output file to %s", *outFileName)
	err := ioutil.WriteFile(*outFileName, []byte(graph.String()), os.FileMode(0644))
	if err != nil {
		log.Fatal(err)
	}

}

func fetchData(connStr string) (results []orderState) {
	log.Println("fetching data")
	log.Println(connStr)
	defer log.Println("data fetched")

	db, err := sql.Open("postgres", connStr)
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
			State: fmt.Sprintf(`"%s"`, stateNames[state])})
	}

	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}

	return
}
