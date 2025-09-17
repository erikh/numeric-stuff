package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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
	Entries uint  `json:"line_count"`
	Total   int64 `json:"total_amount"`
}

func fetchPage(doneChan chan bool, offsetChan <-chan uint, entryChan chan []Entry) {
	defer func() {
		doneChan <- true
		log.Print("sent done notification")
	}()
	c := &http.Client{}
	for offset := range offsetChan {
		log.Printf("fetching offset %d", offset)
		res, err := c.Get(fmt.Sprintf("https://t1.numeric.codes/data/lines?page_size=1&offset=%d&token=%s", offset, Token))
		if err != nil {
			log.Fatalf("Server appears to be down: %s", err)
		}

		result := Result{}

		log.Printf("parsing results for offset %d", offset)
		d := json.NewDecoder(res.Body)
		if err := d.Decode(&result); err != nil {
			log.Fatalf("Json appears to be corrupt: %s", err)
		}

		if len(result.Data) > 0 {
			log.Printf("forwarding result data")
			for _, d := range result.Data {
				log.Printf("offset: %d, ID: %s, acct ID: %s, Amount: %d", offset, d.Id, d.AccountID, d.Amount)
			}
			entryChan <- result.Data
		}

		if !result.Page.HasMore {
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
				log.Printf("received id: %s, ID: %s, Amount: %d", entry.Id, entry.AccountID, entry.Amount)

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
				j, err := json.Marshal(map[string]map[string]*Account{"result": accounts})
				if err != nil {
					log.Fatalf("Error marshaling: %s", err)
				}

				fmt.Println(string(j))

				httpRes, err := http.Post(fmt.Sprintf("https://t1.numeric.codes/schedule/puzzle?token=%s", Token), "application/json", bytes.NewBuffer(j))
				if err != nil {
					log.Fatalf("Error posting: %s", err)
				} else if httpRes.StatusCode != 200 {
					io.Copy(os.Stdout, httpRes.Body)
					fmt.Println()
					log.Fatalf("Bad status: %s", httpRes.Status)
				} else {
					io.Copy(os.Stdout, httpRes.Body)
				}

				return
			}
		}
	}
}
