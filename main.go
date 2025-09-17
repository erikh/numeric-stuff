package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
)

const Token = "1d2c5aca-6940-4022-b10f-f9b5b371fa3b"

type Result struct {
	Data []Entry `json:"data"`
	Page struct {
		Offset  uint `json:"offset"`
		HasMore bool `json:"has_more"`
	}
}

type Entry struct {
	Id        string `json:"id"`
	Amount    int64  `json:"amount"`
	AccountID string `json:"account_id"`
}

type Account struct {
	Entries uint  // 'line_count'
	Total   int64 // sum of amounts
}

func fetchPage(doneChan chan bool, offsetChan <-chan uint, entryChan chan []Entry) {
	defer func() {
		doneChan <- true
		log.Print("sent done notification")
	}()
	c := &http.Client{}
	for offset := range offsetChan {
		log.Printf("fetching offset %d", offset)
		res, err := c.Get(fmt.Sprintf("https://t1.numeric.codes/data/lines?page_size=20&offset=%d&token=%s", offset, Token))
		if err != nil {
			log.Fatalf("Server appears to be down: %s", err)
		}

		result := Result{}

		log.Printf("parsing results for offset %d", offset)
		d := json.NewDecoder(res.Body)
		if err := d.Decode(&result); err != nil {
			log.Fatalf("Json appears to be corrupt: %s", err)
		}

		log.Printf("forwarding result data")
		if len(result.Data) > 0 {
			entryChan <- result.Data
		} else {
			// the has_more parameter lies; there are 110 pages at 20 offset with
			// data but everything past 90 or so has it set to false. I'm going to
			// assume this parameter isn't useful.
			// this means these routines, pooled, are going to overshoot max offset once apiece.
			return
		}
	}
}

const Pool = 50 // number of fetchers to launch

func main() {
	doneChan := make(chan bool, Pool)
	offsetChan := make(chan uint, Pool)
	entryChan := make(chan []Entry, Pool)

	accounts := map[string]*Account{}
	done := 0

	offset := uint(0)
	for i := 0; i < Pool; i++ {
		go fetchPage(doneChan, offsetChan, entryChan)
		// bootstrap the channel so the routines start; will be important for select loop
		offset++
		offsetChan <- offset
	}

	for {
		select {
		case entries := <-entryChan:
			for _, entry := range entries {
				var account *Account
				if res, ok := accounts[entry.AccountID]; ok {
					account = res
				} else {
					account = &Account{}
					accounts[entry.AccountID] = account
				}

				account.Entries++
				account.Total += entry.Amount
			}

			offset++
			offsetChan <- offset
		case <-doneChan:
			done++
			log.Printf("done count: %d", done)
			if done >= Pool {
				e := json.NewEncoder(os.Stdout)
				e.SetIndent("", "\t")
				e.Encode(accounts)
				return
			}
		}
	}
}
