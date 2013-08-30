package main

/*
   Simple command line beanstalkd client

   Allows two modes of operation: push and pull.

   Push: Reads from stdin pipe and writes it to the specified beanstalkd tube
   Pull: Blocks and reserves a job from beanstalkd. Once read, it deletes the
   job from the specified tube.

   Push supports the -m option. Setting this will cause beanc to process each
   line on STDIN as a seperate job rather than as one whole job (the default)

*/
import (
	"flag"
	"fmt"
	"github.com/iwanbk/gobeanstalk"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

var (
	verbose   *bool   = flag.Bool("v", false, "Verbosity")
	multiline *bool   = flag.Bool("m", false, "Creates a new job for each line")
	host      *string = flag.String("host", "127.0.0.1:11300", "The host address and port")
	tube      *string = flag.String("tube", "default", "The tube to use or watch, depending on the action")
	pri       *int    = flag.Int("pri", 0, "The job priority, used when pushing")
	delay     *int    = flag.Int("delay", 0, "The job delay, used when pushing")
	ttr       *int    = flag.Int("ttr", 10, "The job ttr, used when pushing")
)

func main() {
	log.SetFlags(log.Lshortfile)
	log.SetPrefix("beanc:")
	flag.Parse()

	if *verbose {
		fmt.Println("Verbose")
	}

	conn, err := gobeanstalk.Dial(*host)
	if err != nil {
		log.Printf("connect failed")
		log.Fatal(err)
	}

	switch flag.Arg(0) {
	case "push":
		pushCommand(conn)
	case "pull":
		pullCommand(conn)
	default:
		log.Println("Unrecognised command")
		os.Exit(-1)
	}

	os.Exit(0)
}

func pushCommand(conn *gobeanstalk.Conn) {
	if *tube != "" {
		err := conn.Use(*tube)
		if err != nil {
			log.Printf("Use %s failed\n", *tube)
			log.Fatal(err)
		}
	}

	data, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		log.Println("Stdin read error")
		log.Fatal(err)
	}
	if len(data) < 1 {
		log.Println("Stdin has no data")
		log.Fatal()
	}
	if *verbose {
		fmt.Println(string(data))
	}

	var dataset []string
	if *multiline == true {
		dataset = strings.Split(string(data), "\n")
	} else {
		dataset = append(dataset, string(data))
	}

	for _, job := range dataset {
		if len(job) == 0 { // Skip blank lines
			continue
		}

		_, err = conn.Put([]byte(job), *pri, *delay, *ttr)
		if err != nil {
			log.Println("Put failed")
			log.Fatal(err)
		}
	}
}

func pullCommand(conn *gobeanstalk.Conn) {
	var tubesToWatch []string = strings.SplitN(*tube, ",", -1)
	var watchDefault bool = false

	for _, tubeName := range tubesToWatch {
		_, err := conn.Watch(tubeName)
		if err != nil {
			log.Printf("Watch %s failed\n", tubeName)
			log.Fatal(err)
		}
		if tubeName == "default" {
			watchDefault = true
		}
	}

	// As beanstalk by default adds the tube 'default' to the
	// connections watch list, we need to remove it from this
	// connections watch list.
	if watchDefault == false {
		_, err := conn.Ignore("default")
		if err != nil {
			log.Print("Ignore 'default' tube failed\n")
			log.Fatal(err)
		}
	}

	j, err := conn.Reserve()
	if err != nil {
		log.Println("Reserve failed")
		log.Fatal(err)
	}

	err = conn.Delete(j.Id)
	if err != nil {
		log.Printf("Delete failed. Job ID: %d\n", j.Id)
		log.Fatal(err)
	}

	os.Stdout.Write(j.Body)
}
